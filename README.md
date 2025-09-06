# teste_tecnico_butantan
teste técnico para engenheiro de dados.

## 1. Como Executar
Para executar o codigo com sucesso, é necessario varificar se o mesmo está carregando os dados no caminho correto, apos verificação execute o mesmo o arquivo python no terminal de comando.
(codigo contém prints e logica voltada para facilitar e encontrar erros durante a execução do codigo)*DEBUG

## 2. O que o Script Faz
Lê os arquivos CSV de pedidos e itens
Identifica o último preço de compra de cada material
Converte moedas estrangeiras para BRL usando API do Banco Central
Gera relatório final em CSV

## 3. Decisões Técnicas
Principais decisões tecnicas no codigo são voltadas a organização com classes, prints para debug(possivel melhoria com bibliotecas) e cache de memoria, fazendo com que o codigo rode com eficiencia, velocidade e que a API evite bloqueios.

Principal desafio para encontrar o "Último Preço" utilizamos a seguinte logica: 
JOIN: Une tabelas de pedidos e itens
ORDENAÇÃO: Ordena por material e data
GROUP BY + LAST: Pega o registro mais recente de cada material

## 4. Bibliotecas Utilizadas

| Biblioteca | Para que serve                  | Por que escolhemos                       |
|------------|---------------------------------|------------------------------------------|
| pandas     | Manipular dados CSV             | Manipulação e análise de dados            |
| requests   | Consultar API do Banco Central  | Simples e confiável para HTTP             |
| datetime   | Trabalhar com datas             | Nativo do Python, sem dependências extras |
| os         | Acessar sistema operacional     | Carregar os dados da máquina              |

---

## 5. Estrutura do Relatório Final

| COLUNA                   | DESCRIÇÃO                                |
|---------------------------|------------------------------------------|
| codigo_material           | Código único do material                 |
| ultimo_preco_brl          | Preço convertido para Real               |
| ultimo_preco_original     | Preço na moeda original                  |
| moeda_pedido              | Moeda do pedido (BRL, USD, EUR, etc)     |
| data_ultima_compra        | Data da última compra                    |
| codigo_pedido_referencia  | Pedido de onde veio o preço              |
| data_cotacao              | Data da cotação usada (se houver)        |

