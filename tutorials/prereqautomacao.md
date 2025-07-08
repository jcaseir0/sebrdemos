# Automação para a criação dos recursos necessários para a Demonstração

## Configuração do CDP CLI

Criação do ambiente virtual Python local para instalação da biblioteca do CDP CLI.

```shell
cd
python3.12 -m venv cdpcli
cd cdpcli/
source bin/activate
cat "cdpcli" > requirements.txt
(venv) pip install --upgrade pip
(venv) pip -r requirements.txt
python -c "import site; print(site.getsitepackages())"
```
