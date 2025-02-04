import threading, logging, os, time
import jaydebeapi

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Parâmetros de conexão JDBC
JDBC_URL = "jdbc:impala://<knoxendpointserver>:443/;ssl=1;transportMode=http;httpPath=<data_hub_name>/cdp-proxy-api/impala;AuthMech=3;"
JDBC_JAR = '/path/to/jdbc/ClouderaImpalaJDBC42-2.6.35.1067/ImpalaJDBC42.jar'
# Configurar antes de executar
JDBC_USER = '<workload_user>'
JDBC_PASS = '<workload_password>'

# Lista de consultas SQL complexas com descrições de negócio
QUERIES = [
    {
        "description": "Total de gastos por cliente",
        "query": "SELECT c.id_usuario, c.nome, SUM(t.valor) AS total_gastos FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario GROUP BY c.id_usuario, c.nome ORDER BY total_gastos DESC;"
    },
    {
        "description": "Número de transações por cliente",
        "query": "SELECT c.id_usuario, c.nome, COUNT(t.id_usuario) AS total_transacoes FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario GROUP BY c.id_usuario, c.nome ORDER BY total_transacoes DESC;"
    },
    {
        "description": "Média de gastos por transação para cada cliente",
        "query": "SELECT c.id_usuario, c.nome, AVG(t.valor) AS media_gastos FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario GROUP BY c.id_usuario, c.nome ORDER BY media_gastos DESC;"
    },
    {
        "description": "Clientes com maior valor de transação única",
        "query": "SELECT c.id_usuario, c.nome, MAX(t.valor) AS maior_transacao FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario GROUP BY c.id_usuario, c.nome ORDER BY maior_transacao DESC LIMIT 10;"
    },
    {
        "description": "Total de gastos por categoria",
        "query": "SELECT t.categoria, SUM(t.valor) AS total_gastos FROM bancodemo.transacoes_cartao t GROUP BY t.categoria ORDER BY total_gastos DESC;"
    },
    {
        "description": "Número de transações por status",
        "query": "SELECT t.status, COUNT(*) AS total_transacoes FROM bancodemo.transacoes_cartao t GROUP BY t.status ORDER BY total_transacoes DESC;"
    },
    {
        "description": "Clientes com transações na categoria 'Alimentação'",
        "query": "SELECT DISTINCT c.id_usuario, c.nome FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.categoria = 'Alimentação';"
    },
    {
        "description": "Total de gastos por cliente na categoria 'Transporte'",
        "query": "SELECT c.id_usuario, c.nome, SUM(t.valor) AS total_gastos_transporte FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.categoria = 'Transporte' GROUP BY c.id_usuario, c.nome ORDER BY total_gastos_transporte DESC;"
    },
    {
        "description": "Clientes com transações negadas",
        "query": "SELECT DISTINCT c.id_usuario, c.nome FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.status = 'Negada';"
    },
    {
        "description": "Total de gastos mensais por cliente",
        "query": "SELECT c.id_usuario, c.nome, CONCAT(CAST(YEAR(t.data_transacao) AS STRING), '-', LPAD(CAST(MONTH(t.data_transacao) AS STRING), 2, '0')) AS mes, SUM(t.valor) AS total_gastos FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario GROUP BY c.id_usuario, c.nome, CONCAT(CAST(YEAR(t.data_transacao) AS STRING), '-', LPAD(CAST(MONTH(t.data_transacao) AS STRING), 2, '0')) ORDER BY c.id_usuario, mes;"
    }
]

# Evento para sinalizar o encerramento das threads
stop_event = threading.Event()

def execute_query(query_info):
    description = query_info["description"]
    query = query_info["query"]
    logger.info(f"Executando consulta: {description}")
    try:
        jdbc_user = JDBC_USER
        jdbc_password = JDBC_PASS
        conn = jaydebeapi.connect(
            'com.cloudera.impala.jdbc.Driver',
            JDBC_URL,
            [jdbc_user, jdbc_password],
            JDBC_JAR
        )
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchmany(10)  # Limita a 10 resultados
            logger.info(f"Resultados para a consulta '{description}':")
            cursor.close()
            conn.close()
            time.sleep(5) # Adiciona um atraso de 5 segundos após cada execução
        for row in results:
            logger.info(str(row))
        logger.info("Fim dos resultados\n")
    except Exception as e:
        logger.error(f"Erro ao executar a consulta '{description}': {str(e)}\n")

def run_queries():
    threads = []
    for query_info in QUERIES:
        if stop_event.is_set():
            logger.info("Sinal de parada detectado. Encerrando execução de consultas.")
            break
        thread = threading.Thread(target=execute_query, args=(query_info,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

def check_jdbc_driver():
    if not os.path.exists(JDBC_JAR):
        raise FileNotFoundError(f"JDBC driver not found at {JDBC_JAR}. Please verify the path.")
    logger.info(f"JDBC driver found at {JDBC_JAR}")

def main():
    try:
        logger.info("Iniciando execução das consultas...\n")
        
        check_jdbc_driver()

        logger.info("Conexão com o banco de dados estabelecida.\n")
        run_queries()
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {str(e)}")
    finally:
        stop_event.set()  # Sinaliza para as threads pararem
        logger.info("Todas as consultas foram executadas.")

if __name__ == "__main__":
    main()