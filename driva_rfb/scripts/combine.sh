#!/bin/bash
function merge_and_sort_data() {
	echo 'Mesclando e ordenando Empresas'
	time sudo bash -c "cat *EMPRECSV | sort -k1 > EMPRERFB.csv"
	echo 'Mesclando e ordenando Estabelecimentos'
	time sudo bash -c "cat *ESTABELE | sort -k1 > ESTABELE.csv"
	echo 'Mesclando e ordenando Socios'
	time sudo bash -c "cat *SOCIOCSV | sort -k1 > SOCIORFB.csv"
	echo 'Separando CNPJ por Raiz CNPJ'
	time awk -F ";" '{print $1 ";" $2 ";" $3}' ESTABELE.csv > RAIZ_CNPJ.csv
	
	echo 'Acabou'
}
function join_data(){
	echo 'Join dados de Empresas e Estabelecimento'
	time sudo bash -c "join -t ';' EMPRERFB.csv ESTABELE.csv > RFB.csv"
	echo 'Incluindo Simples'
	sudo mv *SIMPLES.CSV.?????? SIMPLES.csv
	time sudo bash -c "join -a 1 -e NULL -t';' RFB.csv SIMPLES.csv > RFB_completo.csv"
	
	echo 'Colocando CNPJ nos Socios'
	time sudo bash -c "join -t ';' RAIZ_CNPJ.csv SOCIORFB.csv > SOCIORFB_COMPLETO.csv"

}
merge_and_sort_data
join_data