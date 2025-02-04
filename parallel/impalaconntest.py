import jaydebeapi
import os, re

# Parâmetros de conexão
username = '<workload_user>'
password = '<workload_password>'
jar_file = 'ImpalaJDBC42.jar'
jar_path = '/path/to/jdbc/ClouderaImpalaJDBC42-2.6.35.1067'
driver_path = jar_path + '/' + jar_file
jdbc_url = 'jdbc:impala://<knoxendpointserver>/;ssl=1;transportMode=http;httpPath=<data_hub_name>/cdp-proxy-api/impala;AuthMech=3;'


def validar_arquivo_jar(diretorio, nome_arquivo):
    caminho_completo = os.path.join(diretorio, nome_arquivo)
    
    # Verifica se o arquivo existe
    if not os.path.isfile(caminho_completo):
        print(f"O arquivo {nome_arquivo} não existe no diretório especificado.\n")
        return False
    
    # Verifica as permissões de acesso
    if not os.access(caminho_completo, os.R_OK):
        print(f"O arquivo {nome_arquivo} não tem permissão de leitura.\n")
        return False
    
    if not os.access(caminho_completo, os.X_OK):
        print(f"O arquivo {nome_arquivo} não tem permissão de execução.\n")
        return False
    
    print(f"O arquivo {nome_arquivo} existe e tem as permissões corretas.\n")
    return True

def main():
    print("\nValidando arquivo JAR...\n")
    validar_arquivo_jar(jar_path, jar_file)
    try:
        # Estabelecer conexão
        conn = jaydebeapi.connect(
            'com.cloudera.impala.jdbc.Driver',
            jdbc_url,
            [username, password],
            driver_path
        )

        # Criar cursor
        cursor = conn.cursor()

        # Executar uma consulta de teste
        cursor.execute('SHOW DATABASES')

        # Buscar resultados
        results = cursor.fetchall()

        print("\nConexão bem-sucedida!")
        print("\nBancos de dados disponíveis:")
        for row in results:
            print(row[0])

        # Fechar cursor e conexão
        print("\nFechando cursor e conexão...")
        cursor.close()
        conn.close()
        print("\nConexão encerrada com sucesso.")

    except Exception as e:
        print(f"Erro ao conectar: {str(e)}")

if __name__ == "__main__":
    main()