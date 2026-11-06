"""Rede: protocolo de mensagens, peers conhecidos e transporte TCP.

Camada de abstração para a comunicação via TCP/IP. Base de peers conhecidos.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Callable

import helpers

# Cada mensagem é um objeto JSON em uma única linha terminada em "\n".
# Campos comuns a todas: tipo, peer_origem_id, porta, lamport.

HANDSHAKE = "HANDSHAKE"
HANDSHAKE_OK = "HANDSHAKE_OK"
LANCE_PROPOSTA = "LANCE_PROPOSTA"
LANCE_RESPOSTA = "LANCE_RESPOSTA"
VENCEDOR_ANUNCIO = "VENCEDOR_ANUNCIO"
ANUNCIO_OK = "ANUNCIO_OK"
ERRO = "ERRO"

ACEITO = "ACEITO"
RECUSADO = "RECUSADO"

# Construção de mensagens

class ErroProtocolo(ValueError):
    """Mensagem que não é um objeto JSON válido."""

def montar(tipo: str, peer_id: str, porta: int, lamport: int, **campos) -> dict:
    return {"tipo": tipo, "peer_origem_id": peer_id, "porta": porta,
            "lamport": lamport, **campos}


def codificar(mensagem: dict) -> bytes:
    return (json.dumps(mensagem, ensure_ascii=False) + "\n").encode("utf-8")


def decodificar(linha: bytes) -> dict:
    try:
        mensagem = json.loads(linha.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ErroProtocolo(str(e)) from e
    if not isinstance(mensagem, dict):
        raise ErroProtocolo("a mensagem não é um objeto JSON")
    return mensagem

# Registro de peers conhecidos

def parse_endereco(texto) -> tuple[str, int] | None:
    """Converte 'ip:porta' em (ip, porta). Retorna None se for inválido."""
    try:
        host, porta = str(texto).strip().rsplit(":", 1)
        porta = int(porta)
    except (ValueError, AttributeError):
        return None
    if not host or not 1 <= porta <= 65535:
        return None
    return host, porta


class RegistroPeers:
    """Endereço "ip:porta" -> id do peer (None até o primeiro contato).

    Os métodos que alteram o registro devolvem True quando algo mudou.
    """

    def __init__(self, meu_id: str, enderecos=()) -> None:
        self.meu_id = meu_id
        self._peers: dict[str, str | None] = {e: None for e in enderecos}

    def __len__(self) -> int:
        return len(self._peers)

    def enderecos(self) -> list[str]:
        return list(self._peers)

    def itens(self) -> list[tuple[str, str | None]]:
        return list(self._peers.items())

    def ids(self) -> set[str]:
        return {i for i in self._peers.values() if i}

    def get_label(self, endereco: str) -> str:
        return f"{self._peers.get(endereco) or '?'}@{endereco}"

    def to_list(self) -> list[dict]:
        return [{"id": i, "endereco": e} for e, i in self._peers.items() if i]

    def adicionar(self, endereco: str) -> bool:
        """Adiciona um endereço ainda sem id. True se era novo."""
        if parse_endereco(endereco) is None or endereco in self._peers:
            return False
        self._peers[endereco] = None
        return True

    def remover(self, endereco: str) -> None:
        self._peers.pop(endereco, None)

    def confirmar(self, endereco: str, peer_id: str) -> bool:
        """
        Contato direto bem-sucedido: 'endereco' pertence a 'peer_id'.

        Remove o endereço se ele for deste próprio peer e elimina endereços
        duplicados do mesmo id.
        """
        if peer_id == self.meu_id:
            self._peers.pop(endereco, None)
            return False
        for outro, ident in list(self._peers.items()):
            if ident == peer_id and outro != endereco:
                del self._peers[outro]
        if self._peers.get(endereco) == peer_id:
            return False
        self._peers[endereco] = peer_id
        return True

    def aprender(self, endereco: str, peer_id: str) -> bool:
        """Peer conhecido indiretamente. Só registra se ainda for desconhecido."""
        if parse_endereco(endereco) is None or not peer_id or peer_id == self.meu_id:
            return False
        if peer_id in self._peers.values() or self._peers.get(endereco) is not None:
            return False
        self._peers[endereco] = peer_id
        return True


# transporte TCP

# Recebe (mensagem, ip_do_remetente) e devolve a resposta. Deve ser síncrono:
# assim o processamento de uma mensagem nunca é interrompido por outra.
Tratador = Callable[[dict, str], dict]

@dataclass(frozen=True)
class Entrega:
    """Resultado do envio de uma mensagem a um endereço."""
    endereco: str
    resposta: dict | None = None
    erro: str | None = None


class Transporte:
    """
    Servidor TCP que responde mensagens e cliente que envia com timeout.

    Cada conexão transporta uma mensagem e uma resposta.
    """

    def __init__(self, porta: int, tratador: Tratador,
                timeout: float = 3.0, verbose: bool = False) -> None:
        self.porta = porta
        self.timeout = timeout
        self.verbose = verbose
        self._tratador = tratador
        self._servidor: asyncio.AbstractServer | None = None

    async def iniciar(self, host: str = "0.0.0.0") -> None:
        self._servidor = await asyncio.start_server(self._atender, host, self.porta)

    async def parar(self) -> None:
        if self._servidor is not None:
            self._servidor.close()
            await self._servidor.wait_closed()

    async def _atender(self, reader, writer) -> None:
        peer = writer.get_extra_info("peername")
        ip = peer[0] if peer else "?"
        try:
            linha = await asyncio.wait_for(reader.readline(), self.timeout)
            if not linha:
                return
            self._mostrar("recebido de", ip, linha)
            resposta = self._tratador(decodificar(linha), ip)
            writer.write(codificar(resposta))
            await writer.drain()
        except ErroProtocolo:
            helpers.aviso(f"Mensagem inválida recebida de {ip}")
        except (OSError, asyncio.TimeoutError):
            pass
        finally:
            await _fechar(writer)

    async def enviar(self, endereco: str, mensagem: dict) -> Entrega:
        destino = parse_endereco(endereco)
        if destino is None:
            return Entrega(endereco, erro="endereço inválido")

        writer = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(*destino), self.timeout)
            writer.write(codificar(mensagem))
            await writer.drain()

            linha = await asyncio.wait_for(reader.readline(), self.timeout)
            if not linha:
                return Entrega(endereco, erro="conexão fechada sem resposta")
            self._mostrar("resposta de", endereco, linha)
            return Entrega(endereco, resposta=decodificar(linha))

        except asyncio.TimeoutError:
            return Entrega(endereco, erro="timeout")
        except OSError as e:
            return Entrega(endereco, erro=type(e).__name__)
        except ErroProtocolo:
            return Entrega(endereco, erro="resposta inválida")
        finally:
            if writer is not None:
                await _fechar(writer)

    async def difundir(self, enderecos, mensagem: dict) -> list[Entrega]:
        """Envia a mesma mensagem a vários endereços, em paralelo."""
        return list(await asyncio.gather(*(self.enviar(e, mensagem) for e in enderecos)))

    def _mostrar(self, direcao: str, origem: str, linha: bytes) -> None:
        if self.verbose:
            helpers.log("JSON", f"{direcao} {origem}: {linha.decode('utf-8', 'replace').strip()}")


async def _fechar(writer) -> None:
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass
