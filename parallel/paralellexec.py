import threading
import jaydebeapi
import logging
import os
import getpass
import jpype

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Parâmetros de conexão JDBC
JDBC_URL = "jdbc:hive2://despark3-master0.jcaseiro.a465-9q4k.cloudera.site:443/;ssl=1;transportMode=http;httpPath=despark3/cdp-proxy-api/hive/bancodemo"
JDBC_DRIVER = "com.cloudera.hive.jdbc.HS2Driver" # jar tvf jdbc/ClouderaHiveJDBC-2.6.25.1033/HiveJDBC42.jar | grep Driver
JDBC_JAR = os.path.abspath("jdbc/ClouderaHiveJDBC-2.6.25.1033/HiveJDBC42.jar")
JDBC_JKS = os.path.abspath("jdbc/gateway-client-trust.jks")

def get_jdbc_user():
    return os.environ.get("HIVE_USER")

# Função para obter a senha de forma segura
def get_password():
    return os.environ.get("HIVE_PASSWORD") or getpass.getpass("Enter Workload password: ")

# Lembre-se de configurar as variáveis de ambiente HIVE_USER e HIVE_PASSWORD de forma segura antes de executar o script.

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

def execute_query(conn, query_info):
    description = query_info["description"]
    query = query_info["query"]
    logger.info(f"Executando consulta: {description}")
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchmany(10)  # Limita a 10 resultados
            logger.info(f"Resultados para a consulta '{description}':")
        for row in results:
            logger.info(str(row))
        logger.info("Fim dos resultados\n")
    except Exception as e:
        logger.error(f"Erro ao executar a consulta '{description}': {str(e)}\n")

def run_queries(conn):
    threads = []
    for query_info in QUERIES:
        thread = threading.Thread(target=execute_query, args=(conn, query_info))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

def check_jdbc_driver():
    if not os.path.exists(JDBC_JAR):
        raise FileNotFoundError(f"JDBC driver not found at {JDBC_JAR}. Please verify the path.")
    logger.info(f"JDBC driver found at {JDBC_JAR}")

def main():
    if not jpype.isJVMStarted():
        check_jdbc_driver()
        jpype.startJVM(jpype.getDefaultJVMPath(), f"-Djava.class.path={JDBC_JAR}")

    jdbc_user = get_jdbc_user()
    jdbc_password = get_password()
    jdbc_url = f"{JDBC_URL};sslTrustStore={JDBC_JKS};trustStorePassword={jdbc_password};user={jdbc_user};PWD={jdbc_password};"

    try:
        conn = jaydebeapi.connect(JDBC_DRIVER, jdbc_url, ['', ''], JDBC_JAR)
        run_queries(conn)
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Todas as consultas foram executadas.")
        jpype.shutdownJVM()

if __name__ == "__main__":
    main()