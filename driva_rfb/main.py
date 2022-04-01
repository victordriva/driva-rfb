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
        False, help="Indica se o download deve ser reiniciado, após uma falha."
    )
):
    """
    Baixa os dados do site da Receita Federal Brasileira.
    """
    typer.echo("Verificando se existe algo novo na Receita")
    if has_new_crawl() or restart:
        typer.echo("Novo crawleamento disponível, iniciando...")
        download_all(restart)
    else:
        typer.echo("Nenhum novo dado disponível")
        raise typer.Abort()


@app.command()
def upload_zip():
    """
    Envia o diretório zippado (como foi baixado) para o Google Cloud Storage.
    """
    timestamp = get_last_modified_date()
    typer.echo(f"Enviando diretório para GCS com data: {timestamp}")
    subprocess.check_output(
        f"gsutil -o GSUtil:parallel_composite_upload_threshold=300M -m cp zip/*.zip gs://driva-lake/crawlers/RFB/{timestamp}/zip",
        shell=True,
    )


@app.command()
def extract():
    """
    Extrai os arquivos baixados.
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
    Processa o diretório compactado gerando o diretório extracted.
    """
    typer.echo("Combinando dados")
    script_path = Path(__file__).parent / "scripts/combine.sh"
    subprocess.check_output(
        f"chmod +x {script_path} && {script_path}", shell=True, cwd="extracted"
    )

    partial_estab = subprocess.check_output(
        "wc -l *.ESTABELE", shell=True, cwd="extracted"
    )
    total_estab = subprocess.check_output(
        "wc -l ESTABELE.csv", shell=True, cwd="extracted"
    )
    typer.echo(f"Parcial de estabelecimentos:\n{partial_estab}\n")
    typer.echo(f"Total de estabelecimentos:\n{total_estab}")

    partial_empre = subprocess.check_output(
        "wc -l *EMPRECSV", shell=True, cwd="extracted"
    )
    total_empre = subprocess.check_output(
        "wc -l EMPRERFB.csv", shell=True, cwd="extracted"
    )
    typer.echo(f"Parcial de empresas:\n{partial_empre}\n")
    typer.echo(f"Total de empresas:\n{total_empre}")

    partial_socio = subprocess.check_output(
        "wc -l *SOCIOCSV", shell=True, cwd="extracted"
    )
    total_socio = subprocess.check_output(
        "wc -l SOCIORFB.csv", shell=True, cwd="extracted"
    )
    typer.echo(f"Parcial de socios:\n{partial_socio}\n")
    typer.echo(f"Total de socios:\n{total_socio}")


@app.command()
def upload_extracted():
    """
    Envia o diretório extracted (após o processamento básico) para o Google Cloud Storage.
    """
    timestamp = get_last_modified_date()
    typer.echo(f"Enviando diretório para GCS com data: {timestamp}")
    subprocess.check_output(
        f"gsutil -o GSUtil:parallel_composite_upload_threshold=300M -m cp extracted/*.csv gs://driva-lake/crawlers/RFB/{timestamp}/extracted",
        shell=True,
    )


@app.command()
def clear_local():
    """
    Apaga qualquer dado gerado pela CLI.
    Cuidado essa ação não pode ser desfeita.
    """
    typer.echo("Limpando diretórios")
    shutil.rmtree("extracted")
    shutil.rmtree("zip")


@app.command()
def all(
    clear: bool = typer.Option(
        True, help="Indica se deve apagar os dados locais, após terminar."
    )
):
    """
    Baixa, extrai e combina todos os dados, enviando os resultados para o Google Cloud Storage.
    Ao final do processo apaga todos os arquivos locais.
    """
    download()
    upload_zip()
    extract()
    combine()
    upload_extracted()
    if clear:
        clear_local()


if __name__ == "__main__":
    app()
