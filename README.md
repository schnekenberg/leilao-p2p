# FASE 1: ESTUDO DA TECNOLOGIA, DESIGN E COMUNICAÇÃO DE PROCESSOS

**Projeto Prático de Laboratório - Sistema de Leilão Distribuído (Arquitetura P2P / Peer-to-Peer)**

**Grupo 5 – Peer-to-Peer (P2P)**

**Integrantes:** Guilherme Schnekenberg Teixeira (RA00336189), Luís Augusto Coelho (RA00331675), Matheus Heimrath Barbosa (RA00333095), Pedro Henrique Vianna Carvalho (RA00333196)

---

## 1. Roteiro Preliminar e Primeira Comunicação (Fase 1)

### 1.1 Tecnologia de Implementação

A POC será desenvolvida em Python 3.12+, utilizando as bibliotecas nativas `socket` e `asyncio` para gerenciamento não-bloqueante de conexões TCP e concorrência, e `json` para serialização das mensagens. A escolha por bibliotecas padrão garante facilidade de execução entre os notebooks dos integrantes sem dependências externas complexas.

### 1.2 Teste de Primeira Comunicação Entre Dois Processos

Na aula de laboratório do dia 30/10, o Grupo 5 validou com sucesso a primeira comunicação P2P entre dois processos isolados (Peer 1 na porta 5001 e Peer 2 na porta 5002). O fluxo de teste consistiu em:

- **Passo 1 - Inicialização do Servidor Local:** O Peer 1 iniciou a escuta na porta 5001 e o Peer 2 na porta 5002.
- **Passo 2 - Conexão Direta (Socket TCP):** O Peer 1 conectou-se à porta 5002 do Peer 2 e enviou a mensagem de HANDSHAKE.
- **Passo 3 - Troca de Mensagem de Lance:** O Peer 1 enviou uma mensagem JSON com `'tipo': 'LANCE_PROPOSTA'` e `'valor': 1200.00`. O Peer 2 recebeu, validou que R$ 1200,00 era maior que o lance mínimo inicial (R$ 1000,00), atualizou o seu estado local e retornou `'LANCE_RESPOSTA'` com `'status': 'ACEITO'`.
- **Passo 4 - Validação da Comunicação:** Ambos os consoles exibiram o log de sucesso da transação, comprovando o funcionamento da comunicação direta bidirecional sem servidor central.

---

## 2. Divisão de Tarefas e Cronograma de Execução

| Integrante | Responsabilidade | Entregáveis Específicos na POC |
| :--- | :--- | :--- |
| **Guilherme Schnekenberg** | Arquitetura P2P e Servidor Socket Async + Documentação | Desenvolvimento do módulo de escuta TCP (`PeerServer`), controlo de porta e aceitação de conexões assíncronas |
| **Luís Augusto Coelho** | Módulo Cliente, CLI e Propagação de Lances | Desenvolvimento do módulo `PeerClient`, interface de linha de comando para inserção de lances e broadcast para a lista de pares |
| **Matheus Heimrath** | Gerenciamento de Estado Local e Protocolo JSON | Implementação da estrutura de dados do leilão em memória, validação das regras de negócio e serialização do protocolo JSON |
| **Pedro Henrique Vianna** | Experimentos (Falhas/Concorrência) | Criação dos cenários de teste de concorrência (lances simultâneos), experimento de queda de nó (RE2/RE3) |

### 2.1 Planejamento para as Próximas Fases

- **Laboratório 2 (06/11):** Implementação do consenso simples para encerramento do leilão (RF5), execução do experimento de falha/queda de nó (RE2) e teste de lances simultâneos (RE3). Finalização do repositório GitHub e elaboração dos slides.
- **Seminário (13/11):** Apresentação prática da POC a rodar em no mínimo 2 notebooks em sala de aula, demonstrando o leilão P2P em tempo real para a turma e para o professor Carlos Paes.
