import os
import shutil
import subprocess
from multiprocessing import Pool
from pathlib import Path

import typer

from .download import download_all, get_last_modified_date, has_new_crawl

app = typer.Typer()


@app.command()
def download(
    restart: bool = typer.Option(
        False, help="Restart the download of files that failed previously."
    )
):
    """
    Use esse comando para baixar os dados do site da Receita Federal Brasileira.
    """
    typer.echo("Verificando se existe algo novo na Receita")
    if has_new_crawl():
        typer.echo("Novo crawleamento disponível, iniciando...")
        download_all(restart)
    else:
        typer.echo("Nenhum novo dado disponível")
        raise typer.Abort()


@app.command()
def upload_zip():
    """
    Use esse comando para enviar o diretório zippado (como foi baixado) para o Google Cloud Storage
    """
    timestamp = get_last_modified_date()
    typer.echo(f"Enviando diretório para GCS com data: {timestamp}")
    subprocess.check_output(
        f"gsutil -m cp zip/*.zip gs://driva-lake/crawlers/RFB/{timestamp}/zip",
        shell=True,
    )


@app.command()
def extract():
    """
    Use esse comando para extrair os arquivos baixados.
    """
    typer.echo("Extraindo arquivos")
    os.makedirs("extracted", exist_ok=True)
    files = [f"zip/{f}" for f in os.listdir("zip") if f.endswith(".zip")]
    with Pool(processes=4) as pool:
        pool.map(extract_file, files)


def extract_file(file):
    subprocess.check_output(f"unzip {file} -d extracted", shell=True)


@app.command()
def combine():
    """
    Use esse comando para processar o diretório zippado gerando o diretório extraído
    """
    typer.echo(f"Combinando dados")
    script_path = Path(__file__).parent / "scripts/combine.sh"
    subprocess.check_output(
        f"chmod +x {script_path} && {script_path}", shell=True, cwd="extracted"
    )


@app.command()
def upload_extracted():
    """
    Use esse comando para enviar o diretório extraído (após o processamento básico) para o Google Cloud Storage
    """
    timestamp = get_last_modified_date()
    typer.echo(f"Enviando diretório para GCS com data: {timestamp}")
    subprocess.check_output(
        f"gsutil -o GSUtil:parallel_composite_upload_threshold=300M -m cp extracted/*.csv gs://driva-lake/crawlers/RFB/{timestamp}/extracted",
        shell=True,
    )


@app.command()
def clean():
    """
    Use esse comando para apagar qualquer dado gerado pela CLI.
    """
    typer.echo("Limpando diretórios")
    shutil.rmtree("extracted")
    shutil.rmtree("zip")


@app.command()
def all():
    """
    Use esse comando para baixar, extrair e combinar todos os dados.
    """
    download()
    upload_zip()
    extract()
    combine()
    upload_extracted()
    clean()


if __name__ == "__main__":
    app()
