"""Lógica do peer: orquestra rede, leilão, relógio e consenso.

Não lê o teclado nem interpreta argumentos: quem faz isso é o app.py.
Os logs impressos aqui são eventos do sistema (lances, sincronização,
falhas, encerramento), não interface.

Concorrência dentro do peer: o asyncio roda em uma única thread e só troca de
tarefa em um "await". O tratamento de mensagens (_tratar e os métodos _em_*)
é síncrono, então nunca é interrompido no meio: duas mensagens que chegam
juntas são processadas uma de cada vez. Por isso não há Lock.
Entre peers diferentes não existe exclusão mútua: a convergência vem da
regra determinística leilao.supera().
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass

import helpers
import rede
from helpers import RelogioLamport
from leilao import ColetorAnuncios, Lance, Leilao, ResultadoLance, descrever
from rede import Entrega, RegistroPeers, Transporte


# parâmetros de config do peer

@dataclass(frozen=True)
class Config:
    peer_id: str
    porta: int
    peers: tuple[str, ...] = ()
    item: str = "Notebook Gamer High-End"
    lance_minimo: float = 1000.0
    encerra_em: float | None = None
    espera_anuncios: float = 5.0
    sem_desempate: bool = False
    verbose: bool = False
    timeout: float = 3.0



# orquestrador

class Peer:
    """Coordena rede, leilão, peers, relógio e consenso de um peer."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.id = cfg.peer_id
        self.relogio = RelogioLamport()
        self.leilao = Leilao(cfg.item, cfg.lance_minimo, cfg.encerra_em,
                             desempate=not cfg.sem_desempate)
        self.peers = RegistroPeers(cfg.peer_id, cfg.peers)
        self.anuncios = ColetorAnuncios(cfg.peer_id)
        self.transporte = Transporte(cfg.porta, self._tratar, cfg.timeout, cfg.verbose)

        self._tratadores = {
            rede.HANDSHAKE: self._em_handshake,
            rede.LANCE_PROPOSTA: self._em_lance,
            rede.VENCEDOR_ANUNCIO: self._em_anuncio,
        }
        self._tarefa_encerramento: asyncio.Task | None = None

    # ciclo de vida

    async def iniciar(self) -> None:
        """Abre o servidor, sincroniza com os peers e agenda o encerramento."""
        await self.transporte.iniciar()
        await self.sincronizar()
        self._tarefa_encerramento = asyncio.create_task(self._monitorar_encerramento())

    async def parar(self) -> None:
        if self._tarefa_encerramento is not None:
            self._tarefa_encerramento.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._tarefa_encerramento
        await self.transporte.parar()

    # ações do menu

    async def dar_lance(self, valor: float) -> None:
        lance = Lance(valor, self.id, self.relogio.evento())

        # 1) valida e aplica localmente
        resultado = self.leilao.avaliar(lance)
        if not resultado.aceito:
            helpers.log("LANCE", f"Lance recusado localmente: {resultado.motivo}")
            return

        # 2) envia diretamente a todos os peers
        helpers.log("LANCE", f"Meu lance {lance} foi aceito localmente. "
                             f"Enviando a {len(self.peers)} peer(s)...")
        respostas = await self._difundir(self._mensagem(rede.LANCE_PROPOSTA, lance=lance.to_dict()))

        aceitos = recusados = falhas = 0
        for endereco, resposta in respostas:
            if resposta is None:
                falhas += 1
                continue
            label = self.peers.get_label(endereco)
            if resposta.get("status") == rede.ACEITO:
                aceitos += 1
                helpers.log("LANCE", f"  {label}: ACEITO")
            else:
                recusados += 1
                helpers.log("LANCE", f"  {label}: RECUSADO ({resposta.get('motivo')})")
            self._mesclar(resposta.get("estado")) # aprende lances que um peer não conhece

        helpers.log("LANCE", f"Resultado: {aceitos} aceitaram, {recusados} recusaram, "
                             f"{falhas} inacessível(is)")
        if self.leilao.maior_lance != lance:
            helpers.aviso(f"Seu lance foi superado. Maior lance: {descrever(self.leilao.maior_lance)}")

    async def conectar(self, endereco: str) -> bool:
        """Handshake manual com um novo endereço."""
        novo = self.peers.adicionar(endereco)
        conectou = await self._handshake_com([endereco]) > 0
        if not conectou and novo:
            self.peers.remover(endereco)
        return conectou

    async def sincronizar(self) -> None:
        """Handshake com todos os peers conhecidos."""
        if not len(self.peers):
            helpers.info("Nenhum peer conhecido. Use a opção 3 do menu para adicionar um.")
            return
        ok = await self._handshake_com(self.peers.enderecos())
        helpers.log("PEER", f"Handshake: {ok} peer(s) responderam ({len(self.peers)} conhecido(s))")

    # mensagens recebidas

    def _tratar(self, msg: dict, ip: str) -> dict:
        """Ponto de entrada de toda mensagem recebida (chamado pelo transporte)."""
        self.relogio.recebimento(msg.get("lamport"))

        origem = str(msg.get("peer_origem_id") or "?")
        porta = msg.get("porta")
        if origem != "?" and isinstance(porta, int) and 1 <= porta <= 65535:
            endereco = f"{ip}:{porta}"
            if self.peers.aprender(endereco, origem):
                helpers.log("PEER", f"Peer descoberto: {origem} em {endereco}")

        tratador = self._tratadores.get(msg.get("tipo"))
        if tratador is None:
            return self._mensagem(rede.ERRO, mensagem=f"tipo de mensagem desconhecido: {msg.get('tipo')}")
        return tratador(msg, origem)

    def _em_handshake(self, msg: dict, origem: str) -> dict:
        if origem != self.id:
            helpers.log("PEER", f"HANDSHAKE recebido de {origem}")
        self._mesclar(msg.get("estado"))
        return self._mensagem(rede.HANDSHAKE_OK, estado=self.leilao.to_dict(),
                            peers=self.peers.to_list())

    def _em_lance(self, msg: dict, origem: str) -> dict:
        lance = Lance.from_dict(msg.get("lance"))
        if lance is None:
            resultado = ResultadoLance(False, "lance inválido")
        elif lance.peer_id != origem:
            resultado = ResultadoLance(False, "peer_id do lance difere do remetente")
        else:
            resultado = self.leilao.avaliar(lance)

        if resultado.novo_maior:
            helpers.log("LANCE", f"NOVO MAIOR LANCE: {lance}")
        elif not resultado.aceito:
            valor = f"R$ {lance.valor:.2f}" if lance else "?"
            helpers.log("LANCE", f"Recusei lance de {origem} ({valor}): {resultado.motivo}")

        return self._mensagem(
            rede.LANCE_RESPOSTA,
            status=rede.ACEITO if resultado.aceito else rede.RECUSADO,
            motivo=resultado.motivo,
            estado=self.leilao.to_dict(),
        )

    def _em_anuncio(self, msg: dict, origem: str) -> dict:
        bruto = msg.get("lance")
        lance = Lance.from_dict(bruto) if bruto is not None else None
        self.anuncios.registrar(origem, lance)
        helpers.log("FIM", f"{origem} anuncia como vencedor: {descrever(lance)}")

        restante = self.leilao.segundos_restantes()
        if not self.leilao.encerrado and restante:
            helpers.aviso(f"Pelo meu relógio ainda faltam {restante:.1f}s para o encerramento. "
                          "Os relógios das máquinas podem estar dessincronizados.")
        elif self.anuncios.verificado:
            opiniao = "concorda" if self.anuncios.concorda_comigo(lance) else "DISCORDA"
            helpers.log("FIM", f"Anúncio tardio de {origem}: {opiniao} do meu resultado")

        return self._mensagem(rede.ANUNCIO_OK)

    # envio

    def _mensagem(self, tipo: str, **campos) -> dict:
        return rede.montar(tipo, self.id, self.cfg.porta, self.relogio.evento(), **campos)

    async def _difundir(self, msg: dict, enderecos=None) -> list[tuple[str, dict | None]]:
        """Envia a mensagem aos endereços (padrão: todos os peers)."""
        alvos = self.peers.enderecos() if enderecos is None else list(enderecos)
        entregas = await self.transporte.difundir(alvos, msg)
        return [(e.endereco, self._receber_entrega(e)) for e in entregas]

    def _receber_entrega(self, entrega: Entrega) -> dict | None:
        """Processa a resposta de um envio: relógio, registro do peer e falhas."""
        if entrega.resposta is None:
            helpers.aviso(f"{self.peers.get_label(entrega.endereco)} inacessível ({entrega.erro})")
            return None
        resposta = entrega.resposta
        self.relogio.recebimento(resposta.get("lamport"))
        origem = resposta.get("peer_origem_id")
        if origem and self.peers.confirmar(entrega.endereco, str(origem)):
            helpers.log("PEER", f"Peer registrado: {origem} em {entrega.endereco}")
        return resposta

    # ---------------------------------------------------- sincronização

    async def _handshake_com(self, enderecos) -> int:
        """Handshake com os endereços e depois com os peers descobertos por eles."""
        ok, novos = await self._rodada_handshake(enderecos)
        if novos:
            ok_novos, _ = await self._rodada_handshake(novos)
            ok += ok_novos
        return ok

    async def _rodada_handshake(self, enderecos) -> tuple[int, list[str]]:
        if not enderecos:
            return 0, []
        msg = self._mensagem(rede.HANDSHAKE, estado=self.leilao.to_dict())
        ok, novos = 0, []
        for _, resposta in await self._difundir(msg, enderecos):
            if (not resposta or resposta.get("tipo") != rede.HANDSHAKE_OK
                    or resposta.get("peer_origem_id") == self.id):
                continue
            ok += 1
            self._mesclar(resposta.get("estado"))
            for item in resposta.get("peers") or []:
                if not isinstance(item, dict):
                    continue
                endereco, ident = str(item.get("endereco")), str(item.get("id"))
                if self.peers.aprender(endereco, ident):
                    helpers.log("PEER", f"Peer descoberto: {ident} em {endereco}")
                    novos.append(endereco)
        return ok, novos

    def _mesclar(self, remoto) -> None:
        novo_prazo, novo_lance = self.leilao.mesclar(remoto)
        if novo_prazo is not None:
            helpers.log("SYNC", f"Encerramento sincronizado: {helpers.hora(novo_prazo)} (relógio local)")
        if novo_lance is not None:
            helpers.log("SYNC", f"Maior lance agora é {novo_lance}")

    # encerramento (RF5)

    async def _monitorar_encerramento(self) -> None:
        # espera o horário pelo relógio LOCAL (o prazo pode mudar no handshake)
        while not self.leilao.prazo_vencido():
            restante = self.leilao.segundos_restantes()
            espera = 0.5 if restante is None else min(0.5, restante)
            await asyncio.sleep(max(0.01, espera))

        vencedor = self.leilao.encerrar()
        self.anuncios.registrar(self.id, vencedor)
        helpers.bloco("LEILÃO ENCERRADO (pelo relógio desta máquina)", [
            f"Item: {self.leilao.item}",
            f"Vencedor na minha visão: {descrever(vencedor)}",
        ], borda="#")

        await self._difundir(self._mensagem(
            rede.VENCEDOR_ANUNCIO, lance=vencedor.to_dict() if vencedor else None))

        helpers.log("FIM", f"Aguardando {self.cfg.espera_anuncios:.0f}s pelos anúncios dos outros peers...")
        await asyncio.sleep(self.cfg.espera_anuncios)

        helpers.bloco("VERIFICAÇÃO DE CONSENSO SOBRE O VENCEDOR", self.anuncios.verificar(self.peers.ids()))
