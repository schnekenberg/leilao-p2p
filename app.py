"""
Interface do usuário: argumentos de linha de comando, menu e ponto de entrada.

Uso:
  python app.py <ID> <PORTA> --peers IP:PORTA,IP:PORTA [opções]
  
"""

from __future__ import annotations

import argparse
import asyncio
import math
import sys
import threading
import time
from datetime import datetime

import helpers
import rede
from peer import Config, Peer


"""
    Leitor de argumentos da aplicação
    
    Permite a definição de id e porta de um peer, além de configurações do ambiente de leilão 
    
"""
def ler_config(argv=None) -> Config:
    p = argparse.ArgumentParser(prog="app.py", description="Peer do leilão distribuído P2P")
    p.add_argument("id", help="identificador único do peer (ex.: Peer_1)")
    p.add_argument("porta", type=int, help="porta TCP local (ex.: 5001)")
    p.add_argument("--peers", default="",
                   help="outros peers separados por vírgula (ex.: 192.168.0.10:5002,192.168.0.11:5003)")
    fim = p.add_mutually_exclusive_group()
    fim.add_argument("--duracao", type=float, help="duração do leilão em segundos, a partir de agora")
    fim.add_argument("--encerra-em", help="horário de encerramento HH:MM ou HH:MM:SS (relógio local)")
    p.add_argument("--item", default=Config.item, help="descrição do item")
    p.add_argument("--minimo", type=float, default=Config.lance_minimo, help="lance mínimo em R$")
    p.add_argument("--espera-anuncios", type=float, default=Config.espera_anuncios,
                   help="segundos aguardando anúncios de vencedor (padrão 5)")
    p.add_argument("--sem-desempate", action="store_true",
                   help="desliga o desempate determinístico (experimento RE3)")
    p.add_argument("-v", "--verbose", action="store_true", help="mostra o JSON bruto das mensagens")
    a = p.parse_args(argv)

    if not 1 <= a.porta <= 65535:
        p.error("porta inválida (1 a 65535)")
    if not math.isfinite(a.minimo) or a.minimo <= 0:
        p.error("lance mínimo inválido")

    peers = tuple(s.strip() for s in a.peers.split(",") if s.strip())
    for endereco in peers:
        if rede.parse_endereco(endereco) is None:
            p.error(f"endereço de peer inválido: {endereco}")

    encerra_em = None
    if a.duracao is not None:
        if not math.isfinite(a.duracao) or a.duracao <= 0:
            p.error("a duração deve ser positiva")
        encerra_em = time.time() + a.duracao
    elif a.encerra_em:
        formato = "%H:%M:%S" if a.encerra_em.count(":") == 2 else "%H:%M"
        try:
            hora = datetime.strptime(a.encerra_em, formato).time()
        except ValueError:
            p.error("use --encerra-em HH:MM ou HH:MM:SS")
        alvo = datetime.combine(datetime.now().date(), hora)
        if alvo <= datetime.now():
            p.error("esse horário já passou")
        encerra_em = alvo.timestamp()

    return Config(peer_id=a.id, porta=a.porta, peers=peers, item=a.item,
                  lance_minimo=a.minimo, encerra_em=encerra_em,
                  espera_anuncios=max(0.0, a.espera_anuncios),
                  sem_desempate=a.sem_desempate, verbose=a.verbose)


class LeitorTeclado:
    """Lê o teclado em uma thread daemon e entrega as linhas ao event loop.

    Uma thread daemon não impede o programa de fechar com Ctrl+C, ao
    contrário de asyncio.to_thread(input), que fica preso esperando Enter.
    """

    def __init__(self) -> None:
        self._fila: asyncio.Queue[str | None] = asyncio.Queue()
        loop = asyncio.get_running_loop()
        threading.Thread(target=self._ler, args=(loop,), daemon=True).start()

    def _ler(self, loop) -> None:
        try:
            for linha in sys.stdin:
                self._entregar(loop, linha.rstrip("\r\n"))
        except Exception:
            pass
        self._entregar(loop, None) # fim da entrada

    def _entregar(self, loop, valor) -> None:
        try:
            loop.call_soon_threadsafe(self._fila.put_nowait, valor)
        except RuntimeError:
            pass # event loop já encerrado

    async def ler(self, prompt: str) -> str:
        print(prompt, end="", flush=True)
        linha = await self._fila.get()
        if linha is None:
            raise EOFError
        return linha.strip()


class Menu:
    def __init__(self, peer: Peer) -> None:
        self.peer = peer
        self.teclado = LeitorTeclado()
        self.opcoes = {
            "1": ("Dar um lance (envia a todos os peers)", self._dar_lance),
            "2": ("Ver estado do leilão", self._mostrar_estado),
            "3": ("Conectar a um novo peer (HANDSHAKE)", self._conectar),
            "4": ("Ressincronizar com todos os peers", self.peer.sincronizar),
        }

    async def executar(self) -> None:
        while True:
            print("\n" + "=" * 40)
            print(f"   LEILÃO P2P - {self.peer.id} (porta {self.peer.cfg.porta})")
            print("=" * 40)
            for chave, (descricao, _) in self.opcoes.items():
                print(f"{chave}. {descricao}")
            print("0. Sair")

            try:
                escolha = await self.teclado.ler("\nEscolha uma opção: ")
                if escolha == "":
                    continue
                if escolha == "0":
                    helpers.info(f"Encerrando {self.peer.id}...")
                    return
                opcao = self.opcoes.get(escolha)
                if opcao is None:
                    print("Opção inválida.")
                    continue
                await opcao[1]()
            except EOFError:
                return

    async def _dar_lance(self) -> None:
        texto = await self.teclado.ler("Valor do lance (R$): ")
        try:
            valor = float(texto.replace(",", "."))
        except ValueError:
            valor = math.nan
        if not math.isfinite(valor) or valor <= 0:
            print("Informe um valor numérico positivo.")
            return
        await self.peer.dar_lance(valor)

    async def _conectar(self) -> None:
        endereco = await self.teclado.ler("Endereço do peer (IP:PORTA): ")
        if rede.parse_endereco(endereco) is None:
            print("Endereço inválido. Exemplo: 192.168.0.10:5002")
            return
        if await self.peer.conectar(endereco):
            helpers.info(f"Conectado a {self.peer.peers.get_label(endereco)}")
        else:
            helpers.aviso(f"Não foi possível conectar a {endereco}")

    async def _mostrar_estado(self) -> None:
        leilao, peers = self.peer.leilao, self.peer.peers

        linhas = [
            f"Item:            {leilao.item}",
            f"Lance mínimo:    R$ {leilao.lance_minimo:.2f}",
        ]
        if leilao.maior_lance:
            linhas += [f"Maior lance:     R$ {leilao.maior_lance.valor:.2f}",
                       f"Dado por:        {leilao.maior_lance.peer_id}"]
        else:
            linhas.append("Maior lance:     nenhum")

        if leilao.encerrado:
            linhas.append("Situação:        ENCERRADO")
        elif leilao.encerra_em is None:
            linhas.append("Encerramento:    não definido")
        else:
            restante = int(leilao.segundos_restantes())
            linhas += [f"Encerramento:    {helpers.hora(leilao.encerra_em)} (relógio local)",
                       f"Tempo restante:  {restante // 60:02d}:{restante % 60:02d}"]

        linhas.append(f"Relógio Lamport: {self.peer.relogio.tempo}")
        linhas.append("Peers conhecidos:")
        linhas += [f"  - {ident or '?'} em {end}" for end, ident in peers.itens()] or ["  (nenhum)"]

        helpers.bloco("ESTADO LOCAL DO LEILÃO", linhas, borda="-")


# ponto de entrada

async def executar(cfg: Config) -> None:
    linhas = [
        f"Identificação: {cfg.peer_id}",
        f"Escutando em:  0.0.0.0:{cfg.porta}",
        f"Item:          {cfg.item} (mínimo R$ {cfg.lance_minimo:.2f})",
    ]
    if cfg.sem_desempate:
        linhas.append("MODO EXPERIMENTO: desempate determinístico DESLIGADO")
    helpers.bloco("PEER INICIADO", linhas)

    peer = Peer(cfg)
    try:
        await peer.iniciar()
    except OSError as e:
        sys.exit(f"Não foi possível abrir a porta {cfg.porta}: {e}")

    if peer.leilao.encerra_em is None:
        helpers.aviso("Horário de encerramento ainda não definido. "
                      "Ele será recebido de outro peer no handshake.")
    else:
        helpers.info(f"Encerramento às {helpers.hora(peer.leilao.encerra_em)} (relógio local)")

    try:
        await Menu(peer).executar()
    finally:
        await peer.parar()


def main(argv=None) -> None:
    cfg = ler_config(argv)
    try:
        asyncio.run(executar(cfg))
    except KeyboardInterrupt:
        print("\nPeer encerrado pelo teclado.")


if __name__ == "__main__":
    main()
