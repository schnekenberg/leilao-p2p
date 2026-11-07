"""
Modelo do leilão: o tipo Lance, as regras, o estado local e o consenso.
"""

from __future__ import annotations

import math
import time
from dataclasses import asdict, dataclass


# Lance

@dataclass(frozen=True)
class Lance:
    valor: float
    peer_id: str
    lamport: int

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, bruto) -> Lance | None:
        """Valida um lance vindo da rede. Retorna None se for inválido."""
        if not isinstance(bruto, dict):
            return None
        try:
            valor = float(bruto["valor"])
            lamport = int(bruto["lamport"])
            peer_id = str(bruto["peer_id"])
        except (KeyError, TypeError, ValueError):
            return None
        if not math.isfinite(valor) or valor <= 0 or lamport < 0 or not peer_id:
            return None
        return cls(valor, peer_id, lamport)

    def __str__(self) -> str:
        return f"R$ {self.valor:.2f} de {self.peer_id} (Lamport={self.lamport})"


def descrever(lance: Lance | None) -> str:
    return str(lance) if lance else "nenhum lance"


def supera(novo: Lance, atual: Lance | None, desempate: bool = True) -> bool:
    """Decide se o lance 'novo' substitui o lance 'atual'.

    1. Valor maior vence.
    2. Empate de valor: vence o MENOR relógio de Lamport (o lance que
       "aconteceu antes" na ordem lógica); persistindo o empate, vence o
       menor id de peer.

    Como todos os peers aplicam a mesma regra, todos convergem para o mesmo
    vencedor, qualquer que seja a ordem de chegada das mensagens.
    Com desempate=False, no empate fica o lance que chegou primeiro em cada
    peer, e os peers podem divergir (experimento RE3).
    """
    if atual is None:
        return True
    if novo.valor != atual.valor:
        return novo.valor > atual.valor
    if not desempate:
        return False
    return (novo.lamport, novo.peer_id) < (atual.lamport, atual.peer_id)


# Armazenamento do estado local

@dataclass(frozen=True)
class ResultadoLance:
    aceito: bool
    motivo: str
    novo_maior: bool = False


class Leilao:
    def __init__(self, item: str, lance_minimo: float,
                encerra_em: float | None = None, desempate: bool = True) -> None:
        self.item = item
        self.lance_minimo = lance_minimo
        self.encerra_em = encerra_em
        self.desempate = desempate
        self.maior_lance: Lance | None = None
        self.encerrado = False

    def prazo_vencido(self) -> bool:
        return self.encerra_em is not None and time.time() >= self.encerra_em

    def aberto(self) -> bool:
        return not self.encerrado and not self.prazo_vencido()

    def segundos_restantes(self) -> float | None:
        if self.encerra_em is None:
            return None
        return max(0.0, self.encerra_em - time.time())

    def to_dict(self) -> dict:
        return {
            "item": self.item,
            "lance_minimo": self.lance_minimo,
            "maior_lance": self.maior_lance.to_dict() if self.maior_lance else None,
            "encerra_em": self.encerra_em,
            "encerrado": self.encerrado,
        }

    def avaliar(self, lance: Lance) -> ResultadoLance:
        """Aplica as regras ao lance e, se aceito, atualiza o maior lance."""
        if not self.aberto():
            return ResultadoLance(False, "leilão encerrado")

        atual = self.maior_lance
        if lance == atual:
            return ResultadoLance(True, "lance já conhecido")
        if atual is None and lance.valor < self.lance_minimo:
            return ResultadoLance(False, f"abaixo do lance mínimo (R$ {self.lance_minimo:.2f})")
        if not supera(lance, atual, self.desempate):
            return ResultadoLance(False, f"não supera o maior lance atual ({atual})")

        self.maior_lance = lance
        return ResultadoLance(True, "novo maior lance", novo_maior=True)

    def mesclar(self, remoto) -> tuple[float | None, Lance | None]:
        """Incorpora o estado de outro peer (handshake e respostas de lance).

        Encerramento: vale o horário MAIS CEDO conhecido.
        Maior lance: vale a regra supera(); fica congelado após o encerramento.
        Devolve (novo_prazo, novo_lance), com None no que não mudou.
        """
        if not isinstance(remoto, dict):
            return None, None

        novo_prazo = None
        prazo = _as_float(remoto.get("encerra_em"))
        if prazo is not None and (self.encerra_em is None or prazo < self.encerra_em):
            self.encerra_em = novo_prazo = prazo

        novo_lance = None
        if not self.encerrado:
            lance = Lance.from_dict(remoto.get("maior_lance"))
            if (lance and lance.valor >= self.lance_minimo
                    and supera(lance, self.maior_lance, self.desempate)):
                self.maior_lance = novo_lance = lance

        return novo_prazo, novo_lance

    def encerrar(self) -> Lance | None:
        """Fecha o leilão e devolve o vencedor na visão deste peer."""
        self.encerrado = True
        return self.maior_lance


def _as_float(valor) -> float | None:
    if valor is None:
        return None
    try:
        valor = float(valor)
    except (TypeError, ValueError):
        return None
    return valor if math.isfinite(valor) else None


# Consenso

class ColetorAnuncios:
    """Guarda o vencedor anunciado por cada peer e compara com o meu.

    A verificação apenas DETECTA divergência; corrigi-la exigiria um
    algoritmo de consenso (Paxos, Raft), fora do escopo da POC.
    """

    def __init__(self, meu_id: str) -> None:
        self.meu_id = meu_id
        self.verificado = False
        self._anuncios: dict[str, Lance | None] = {}

    def registrar(self, peer_id: str, lance: Lance | None) -> None:
        self._anuncios[peer_id] = lance

    def concorda_comigo(self, lance: Lance | None) -> bool:
        return lance == self._anuncios.get(self.meu_id)

    def verificar(self, peers_conhecidos: set[str]) -> list[str]:
        """Compara o anúncio de cada peer com o meu e devolve o relatório em texto."""
        self.verificado = True
        linhas, divergentes, ausentes = [], [], []

        for peer in sorted(set(peers_conhecidos) | set(self._anuncios)):
            if peer not in self._anuncios:
                ausentes.append(peer)
                linhas.append(f"{'sem anúncio':<12} {peer:<12} -")
            elif self.concorda_comigo(self._anuncios[peer]):
                linhas.append(f"{'igual':<12} {peer:<12} {descrever(self._anuncios[peer])}")
            else:
                divergentes.append(peer)
                linhas.append(f"{'DIFERENTE':<12} {peer:<12} {descrever(self._anuncios[peer])}")

        linhas.append("-" * 56)
        if divergentes:
            linhas.append(f"DIVERGÊNCIA: {', '.join(divergentes)} discorda(m) do meu resultado.")
        elif ausentes:
            linhas.append("CONSENSO PARCIAL: quem anunciou concorda, mas faltam "f"anúncios de {', '.join(ausentes)}.")
        else:
            linhas.append("CONSENSO: todos os peers concordam sobre o vencedor.")
        return linhas
