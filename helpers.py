"""Utilitários: relógio lógico de Lamport e impressão no console.

Etiquetas de log usadas no projeto:
    INFO   informações gerais         PEER    descoberta e handshake
    LANCE  lances enviados/recebidos  SYNC   estado sincronizado de outro peer
    FIM    encerramento e consenso    AVISO  falhas e situações anormais
    JSON   mensagens brutas (opção -v)
"""

from datetime import datetime

LARGURA = 60


# relógio de Lamport

class RelogioLamport:
    def __init__(self) -> None:
        self.tempo = 0

    def evento(self) -> int:
        """Evento local ou envio de mensagem: incrementa o relógio."""
        self.tempo += 1
        return self.tempo

    def recebimento(self, tempo_remoto) -> int:
        """Recebimento de mensagem: relógio = max(local, remoto) + 1."""
        try:
            tempo_remoto = int(tempo_remoto)
        except (TypeError, ValueError):
            tempo_remoto = 0
        self.tempo = max(self.tempo, tempo_remoto) + 1
        return self.tempo


# saída no console

def hora(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).strftime("%H:%M:%S")


def log(etiqueta: str, texto: str) -> None:
    """Imprime com o horário do relógio LOCAL desta máquina."""
    print(f"[{datetime.now():%H:%M:%S}] [{etiqueta}] {texto}", flush=True)


def info(texto: str) -> None:
    log("INFO", texto)


def aviso(texto: str) -> None:
    log("AVISO", texto)


def bloco(titulo: str, linhas, borda: str = "=") -> None:
    """Imprime um bloco destacado com título e linhas de conteúdo."""
    print("\n" + borda * LARGURA)
    print(f"  {titulo}")
    print(borda * LARGURA)
    for linha in linhas:
        print(f"  {linha}")
    print(borda * LARGURA + "\n", flush=True)
