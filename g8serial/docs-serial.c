#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct Documento {
  int idD;         
  struct Documento *prox;
  struct Documento *ant;
  double *assunto;
  int armarioPertence;
}Dc;

typedef struct Armario {
  int idA;
  float *peso;
  Dc *listDoc;
  int numDocs;
} Ar;
void calculaPeso(int nArmarios, int nAssuntos, Ar* armarios);

int main(int argc, char *argv[]) {
 float fl;
  FILE *fp;	
  FILE *fr;
  int nArmarios;
  int nDocumentos;
  int nAssuntos;
  int nPesos;
  int i,j;
char *filename;
int num_arm = 0;


	if (argc < 2 || argc > 3) {
        printf("parametro mal passado\n");
        return 1;
    }

    filename = argv[1];
		//abertura do documento
  	fp = fopen(filename, "r");
	if (fp == NULL) {
	printf("Erro ao abrir o arquivo."); 
	}  
    if (argc == 3) {
        num_arm = atoi(argv[2]);
        if (num_arm <= 0) {
            printf("O numero de armarios deve ser positivo.\n");
            return 1;
        }
        
    fscanf(fp, "%d %d %d", &nArmarios, &nDocumentos, &nAssuntos);	
    nArmarios=num_arm;
	}else{
 	 fscanf(fp, "%d %d %d", &nArmarios, &nDocumentos, &nAssuntos);	
	}
	
  // Criação dos armários e pesos
  Ar *armarios = (Ar*) malloc(nArmarios * sizeof(Ar));
  for (i = 0; i < nArmarios; i++) {
    armarios[i].idA = i;
    //alocar memoria para vetor de pesos
    armarios[i].peso = (float*) malloc(nAssuntos * sizeof(float));
    armarios[i].listDoc = NULL;
    armarios[i].numDocs = 0;
  }
  
  // Criação dos documentos e criação de asuntos
  Dc *documentos = (Dc*) malloc(nDocumentos * sizeof(Dc));
  for (i = 0; i < nDocumentos; i++) {
  	int aux;
  	fscanf(fp, "%d\n", &aux);
    documentos[i].idD = aux;
    documentos[i].assunto = (double*) malloc(nAssuntos * sizeof(double));
    for (j = 0; j < nAssuntos; j++) {
     fscanf(fp, "%f", &fl);
      documentos[i].assunto[j] = fl;
    }
    documentos[i].prox = NULL;
    documentos[i].ant = NULL;

    // Adiciona o documento no armário correspondente
    int resto = documentos[i].idD % nArmarios;   //pega o resto da divisão
    Ar *armario = &armarios[resto];            //cria ponteiro que aponta para o endereço armario em que vamos guardar o documento
    documentos[i].prox = armario->listDoc;
	documentos[i].armarioPertence=resto;    //novo documento aponta para o primeiro documento daquele armário
    if (armario->listDoc != NULL) {
   armario->listDoc->ant = &documentos[i];
	}
    armario->listDoc = &documentos[i];      //ponteiro daquele armário aponta para novo documento
    
    armario->numDocs++;
  }
 fclose(fp);
 
//calcula peso
calculaPeso(nArmarios, nAssuntos, armarios);

////////////////////////////////////////////////////////////////
int control;
do{
control=0;
 //Organização dos documentos nos armários com base na distância
  for(i = 0; i < nDocumentos; i++) {
  double menorDistancia = -1;
  int indiceMenorDistancia = -1;
  for (j = 0; j < nArmarios; j++) {
    double distancia = 0;
    int k;
    for (k = 0; k < nAssuntos; k++) {
      double ai = documentos[i].assunto[k];
      double bi = armarios[j].peso[k];
      distancia += (ai - bi) * (ai - bi);
    }
    if (menorDistancia == -1 || distancia < menorDistancia) {
      menorDistancia = distancia;
      indiceMenorDistancia = j;
    }
  }


// Adiciona o documento no armário com a menor distância
	Dc *doc = &documentos[i];
	Ar *armario = &armarios[indiceMenorDistancia];
		if(doc->armarioPertence!=armario->idA)
		control=1;
    
		// Remove o documento da lista do armário anterior, se houver
		armarios[doc->armarioPertence].numDocs--; //diminuir o peso do armario aonde o documento sairá	
		//SE ESTIVER NO MEIO
		if(doc->prox!=NULL&&doc->ant!=NULL){
		 doc->ant->prox = doc->prox;
		  doc->prox->ant = doc->ant;
		}
		else
		if(doc->ant!=NULL){
		doc->ant->prox=NULL;	
		}
		else
		if(doc->prox!=NULL&&doc->ant==NULL){
		doc->prox->ant=NULL;
		armarios[doc->armarioPertence].listDoc=doc->prox;
		}
		else
		armarios[doc->armarioPertence].listDoc=NULL;
		// Insere o documento na lista do armário selecionado
		if (armario->listDoc != NULL) {
		    armario->listDoc->ant = doc;
		    doc->prox=armario->listDoc;
		    doc->ant=NULL;
		    armario->listDoc = doc;	
		}
		else{
		armario->listDoc = doc;	
		doc->prox=NULL;
		doc->ant=NULL;
		}
		armario->numDocs++;//aumenta o numro de documentos
		doc->armarioPertence=armario->idA;//o documento recebe o id do armario no qual ele pertence 	
  	} 
  calculaPeso(nArmarios, nAssuntos, armarios);
  }while(control==1);
  ////////////////////////////////////
  
 // Impressão dos armários e seus documentos
 	char *ponto = strtok(filename, ".");
 	char aux[50];
 	strcpy(aux, ponto);
 	 fr = fopen(strcat(aux,".out"), "w+");
	 for (i = 0; i < nDocumentos; i++) {
	fprintf(fr,"%d %d\n",documentos[i].idD,documentos[i].armarioPertence);
	}
	fclose(fr);
///////////////////////////////////////////////////////////////////////////////////////////////
  // Libera a memória alocada
  for (i = 0; i < nArmarios; i++) {
    free(armarios[i].peso);
  }
  free(armarios);
  for (i = 0; i < nDocumentos; i++) {
    free(documentos[i].assunto);
  }
  free(documentos);
  printf("Classificacao feita com sucesso!!! (^_^)");
  return 0;
}
 //calcular os pesos dos armários
 void calculaPeso(int nArmarios,int nAssuntos, Ar *armarios){
	 int indice;
 double soma=0;
 int k;
 int i;
 int j;
  Dc *docaux;
  

  for (i = 0; i < nArmarios; i++) {
	  if(armarios[i].listDoc==NULL){
	  for(j=0;j<nAssuntos;j++)
	  	armarios[i].peso[j]=0;	  	
	  }else{
  		 for(j=0;j<nAssuntos;j++){
   	 	 docaux = armarios[i].listDoc;
   	  		while (docaux != NULL){
      		 for (k = 0; k < nAssuntos; k++) {
        		if(k==indice)
        		soma=soma+docaux->assunto[k];	
      		}
     		 docaux = docaux->prox;
    		}
    	armarios[i].peso[indice]=soma/armarios[i].numDocs;
    	soma=0;
    	indice++;
  	 }
 		  indice=0;
   	}	
  }
	
}
