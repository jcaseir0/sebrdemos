import threading, logging, os, time
from datetime import datetime
import jaydebeapi

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

log_filename = f"logs/resultadosImpalaParallelExecution_{datetime.now().strftime('%d-%m-%Y-%H-%M')}.log"

# Parâmetros de conexão JDBC
JDBC_URL = "jdbc:impala://<knoxendpointserver>:443/;ssl=1;transportMode=http;httpPath=<data_hub_name>/cdp-proxy-api/impala;AuthMech=3;"
JDBC_JAR = '/path/to/jdbc/ClouderaImpalaJDBC42-2.6.35.1067/ImpalaJDBC42.jar'
# Configurar antes de executar
JDBC_USER = '<workload_user>'
JDBC_PASS = '<workload_password>'

def read_sql_file(file_path):
    queries = []
    with open(file_path, 'r') as file:
        content = file.read()
        query_blocks = content.split('--')
        for block in query_blocks[1:]:  # Skip the first empty block
            lines = block.strip().split('\n')
            description = lines[0].strip()
            query = '\n'.join(lines[1:]).strip()
            if query:
                queries.append({"description": description, "query": query})
    return queries

# Lista de consultas SQL complexas com descrições de negócio
QUERIES = read_sql_file('sql/impalasuboptimal.sql')

# Evento para sinalizar o encerramento das threads
stop_event = threading.Event()

def execute_query(query_info):
    description = query_info["description"]
    query = query_info["query"]
    logger.info(f"Executando consulta: {description}")
    with open(log_filename, 'a') as log_file:
        log_file.write(f"Executando consulta: {description}\n")
        try:
            jdbc_user = 'jcaseiro'
            jdbc_password = 'Cl0ud3r4@2025'
            conn = jaydebeapi.connect(
                'com.cloudera.impala.jdbc.Driver',
                JDBC_URL,
                [jdbc_user, jdbc_password],
                JDBC_JAR
            )
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchmany(10)  # Limita a 10 resultados
                log_file.write(f"Resultados para a consulta '{description}':\n")
                for row in results:
                    log_file.write(f"{str(row)}\n")
                log_file.write("Fim dos resultados\n\n")
            conn.close()
            time.sleep(5) # Adiciona um atraso de 5 segundos após cada execução
        except Exception as e:
            log_file.write(f"Erro ao executar a consulta '{description}': {str(e)}\n\n")

def run_queries_background():
    threads = []
    for query_info in QUERIES:
        if stop_event.is_set():
            logger.info("Sinal de parada detectado. Encerrando execução de consultas.")
            break
        thread = threading.Thread(target=execute_query, args=(query_info,))
        threads.append(thread)
        thread.start()

    # Iniciar uma thread separada para monitorar a conclusão das consultas
    monitor_thread = threading.Thread(target=monitor_queries, args=(threads,))
    monitor_thread.daemon = True
    monitor_thread.start()

def monitor_queries(threads):
    for thread in threads:
        thread.join()

    with open(log_filename, 'a') as log_file:
        log_file.write("Todas as consultas foram concluídas.\n")
    print(f"Todas as consultas foram concluídas. Resultados salvos em {log_filename}")

def check_jdbc_driver():
    if not os.path.exists(JDBC_JAR):
        raise FileNotFoundError(f"JDBC driver not found at {JDBC_JAR}. Please verify the path.")
    logger.info(f"JDBC driver found at {JDBC_JAR}")

def main():
    try:
        logger.info("Iniciando execução das consultas em background...\n")
        
        check_jdbc_driver()

        logger.info("Conexão com o banco de dados estabelecida.\n")
        
        run_queries_background()

        logger.info(f"Execuções iniciadas em background. Resultados serão salvos em {log_filename}\n")
        logger.info("A aplicação será encerrada, mas as consultas continuarão em execução.\n")
    except Exception as e:
        print(f"Erro ao iniciar as consultas: {str(e)}")
        with open(log_filename, 'a') as log_file:
            log_file.write(f"Erro ao iniciar as consultas: {str(e)}\n")
    finally:
        stop_event.set()  # Sinaliza para as threads pararem
        logger.info("Todas as consultas foram executadas.")
        os._exit(0)

if __name__ == "__main__":
    main()