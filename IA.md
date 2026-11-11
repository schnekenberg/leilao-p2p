# Grupo 5 - Uso de Ferramentas de IA

Registro do uso de IA no projeto, conforme a Seção 9 da especificação.

## Ferramentas utilizadas

| Ferramenta | Uso principal |
|---|---|
| Claude | Compreensão do escopo do projeto, auxílio em código e correção de erros |

## Prompts por etapa do código

Os prompts abaixo representam cada etapa do desenvolvimento. Para cada um, registramos o que entrou no código final.

### 1. Estudo da tecnologia

> explique a arquitetura P2P (peer-to-peer) em comparação com cliente/servidor: como funciona, vantagens, desvantagens e exemplos reais de uso em grandes empresas

- **Aproveitado:** entendimento da arquitetura, base de vantagens e desvantagens (sem ponto único de falha, escalabilidade...).

### 2. Protocolo de mensagens

> proponha um formato de mensagens JSON para um leilão P2P em que cada par envia seus lances diretamente aos outros. preciso de mensagens para entrar na rede, dar lance, responder aceito/recusado e anunciar o vencedor no encerramento

- **Aproveitado:** os tipos `HANDSHAKE`/`HANDSHAKE_OK`, `LANCE_PROPOSTA`/`LANCE_RESPOSTA` e `VENCEDOR_ANUNCIO`, com uma linha JSON por mensagem (`rede.py`).
- **Adaptado:** incluímos em todas as mensagens os campos `porta` (para o remetente ser registrado como par) e `lamport` (para ordenar os eventos).

### 3. Comunicação em rede

> em python com asyncio, escreva um par que seja servidor TCP e cliente ao mesmo tempo, troque uma linha JSON por conexão (uma mensagem e uma resposta) e use timeout para detectar quando um par está inacessível

- **Aproveitado:** a base da classe `Transporte` em `rede.py`, com servidor e cliente TCP no mesmo processo.
- **Adaptado:** o envio para todos os pares é feito em paralelo (`difundir`), e cada falha aparece como `[AVISO] ... inacessível` em vez de derrubar o programa.

### 4. Lances concorrentes e ordenação

> dois pares dão o mesmo lance ao mesmo tempo e cada par recebe as mensagens em ordem diferente. como garantir que todos escolham o mesmo vencedor sem um coordenador central? implemente usando relógio de Lamport e uma regra de desempate determinística.

- **Aproveitado:** o `RelogioLamport` (`helpers.py`) e a função `supera()` (`leilao.py`): maior valor vence; no empate, vence o menor Lamport e depois o menor id de par.
- **Adaptado:** criamos a opção `--sem-desempate` para mostrar no experimento RE3 que, sem a regra, os pares divergem.

### 5. Encerramento e consenso

> Adicione um horário (ou duração) de encerramento ao leilão. No horário, o par deve recusar novos lances, anunciar a todos o vencedor que conhece e comparar com os anúncios recebidos dos outros pares para verificar se houve consenso.

- **Aproveitado:** as opções `--duracao` e `--encerra-em`, o monitoramento do encerramento em `peer.py` e a classe `ColetorAnuncios`, que imprime `CONSENSO` ou `DIVERGÊNCIA`.
- **Adaptado:** quando os pares conhecem horários diferentes, vale o mais cedo. Também adicionamos um aviso quando chega um anúncio de vencedor antes do encerramento local, o que indica relógios dessincronizados.

### 6. Tolerância a falhas

> quando um par cai e é reiniciado, ele perde o estado que estava em memória. como recuperar o maior lance e o horário de encerramento a partir dos outros pares?

- **Aproveitado:** o handshake envia o estado completo do leilão, e o método `Leilao.mesclar()` incorpora o maior lance e o horário de encerramento recebidos.
- **Descartado:** a persistência em disco. Ficou como limitação documentada, porque o estado replicado nos outros pares já atende ao experimento RE2.

## Caso em que a IA errou

### handshake só era recebido depois de uma ação no menu

- **O que aconteceu:** em uma versão anterior do código, quando um par enviava HANDSHAKE, o outro par só processava a mensagem depois de escolher uma opção no menu dele. Até lá, a mensagem ficava parada, e quando o outro par escolhia enviar um HANDSHAKE como a ação que quebrasse esse estado congelado, entrava-se em um loop, pois agora o outro par precisa escolher uma ação do menu antes de receber o HANDSHAKE
- **O que a IA respondeu:** quando questionamos esse comportamento, a IA disse que ele era esperado do P2P
- **Como percebemos o problema:** o comportamento contraria a ideia de P2P, em que cada par é servidor o tempo todo e deve responder aos outros a qualquer momento, sem depender do usuário. Também contraria o RF4: os pares precisam ficar sabendo de cada novo lance assim que ele acontece.
- **Causa:** a leitura do teclado (`input()`) bloqueava o event loop do `asyncio`. Enquanto o menu esperava o usuário digitar, o servidor TCP não conseguia atender nenhuma conexão.
- **Como corrigimos:** a leitura do teclado passou para uma thread separada (classe `LeitorTeclado` em `app.py`), que entrega as linhas ao event loop por uma fila. Assim o servidor continua atendendo mensagens enquanto o menu espera.
- **Lição:** a IA pode justificar um bug como comportamento correto. Só percebemos porque comparamos o comportamento com o conceito da arquitetura e com os requisitos.
## Verificação

As informações obtidas com IA foram conferidas em:

- Especificação do projeto (requisitos RF1 a RF6 e RE1 a RE4)
- Slides da disciplina
- Google
- Execução do próprio código, comparando a saída com o que o README descrevia

## Avaliação crítica

A IA acelerou bastante a parte de rede e a organização do código, que levariam mais tempo do que as duas aulas de laboratório permitiam. Os pontos que exigiram mais atenção foram as regras específicas do leilão distribuído; nesses pontos, foi preciso conferir a especificação e testar o comportamento real. O principal aprendizado foi não confiar em resultados descritos pela IA sem executar: ela apresentou previsões como se fossem observações, e só percebemos porque questionamos a origem dos dados.
