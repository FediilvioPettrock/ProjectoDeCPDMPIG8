
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mpi.h"

typedef struct Documento {
  int idD;
  struct Documento *prox;
  struct Documento *ant;
  double *assunto;
  int armarioPertence;
} Dc;

typedef struct Armario {
  int idA;
  float *peso;
  Dc *listDoc;
  int numDocs;
} Ar;

void calculaPeso(int nArmarios, int nAssuntos, Ar* armarios);

int globalControl;
int main(int argc, char *argv[]) {
  float fl;
  FILE *fp;
  FILE *fr;
  int nArmarios;
  int nDocumentos;
  int nAssuntos;
  int nPesos;
  int i, j;
  char *filename;
  int num_arm = 0;

  int rank, size;

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  if (argc < 2 || argc > 3) {
    if (rank == 0)
      printf("parametro mal passado\n");

    MPI_Finalize();
    return 1;
  }

//processo o vai fazer aabertura do documento
  if (rank == 0) {
    filename = argv[1];

    // Abertura do documento
    fp = fopen(filename, "r");
    if (fp == NULL) {
      printf("Erro ao abrir o arquivo.");
      MPI_Finalize();
      return 1;
    }

    if (argc == 3) {
      num_arm = atoi(argv[2]);
      if (num_arm <= 0) {
        printf("O numero de armarios deve ser positivo.\n");
        MPI_Finalize();
        return 1;
      }
      fscanf(fp, "%d %d %d", &nArmarios, &nDocumentos, &nAssuntos);
      nArmarios = num_arm;
    } else {
      fscanf(fp, "%d %d %d", &nArmarios, &nDocumentos, &nAssuntos);
    }
  }

  MPI_Bcast(&nArmarios, 1, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Bcast(&nDocumentos, 1, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Bcast(&nAssuntos, 1, MPI_INT, 0, MPI_COMM_WORLD);

  // Criação dos armários e pesos
  Ar *armarios = (Ar *)malloc(nArmarios * sizeof(Ar));
  for (i = 0; i < nArmarios; i++) {
    armarios[i].idA = i;
    // Alocar memoria para vetor de pesos
    armarios[i].peso = (float *)malloc(nAssuntos * sizeof(float));
    armarios[i].listDoc = NULL;
    armarios[i].numDocs = 0;
  }

  // Criação dos documentos e criação de assuntos
  Dc *documentos = (Dc *)malloc(nDocumentos * sizeof(Dc));
  for (i = 0; i < nDocumentos; i++) {
    int aux;
    if (rank == 0) {
      fscanf(fp, "%d\n", &aux);
      documentos[i].idD = aux;
      documentos[i].assunto = (double *)malloc(nAssuntos * sizeof(double));
      for (j = 0; j < nAssuntos; j++) {
        fscanf(fp, "%f", &fl);
        documentos[i].assunto[j] = fl;
      }
      documentos[i].prox = NULL;
      documentos[i].ant = NULL;
    }

    MPI_Bcast(&aux, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank != 0)
      documentos[i].idD = aux;

    // Adiciona o documento no armário correspondente
    int resto = documentos[i].idD % nArmarios;  // Pega o resto da divisão
    Ar *armario = &armarios[resto];             // Cria ponteiro que aponta para o endereço armario em que vamos guardar o documento

    if (rank == 0) {
      documentos[i].prox = armario->listDoc;
      documentos[i].armarioPertence = resto;

      if (armario->listDoc != NULL) {
        armario->listDoc->ant = &documentos[i];
      }
    }

    MPI_Bcast(&documentos[i].prox, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&documentos[i].armarioPertence, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank != 0) {
      if (documentos[i].prox != NULL)
        documentos[i].prox->ant = &documentos[i];
    }

    MPI_Bcast(&armario->listDoc, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank == 0) {
      armario->listDoc = &documentos[i];
      armario->numDocs++;
    }

    MPI_Bcast(&armario->numDocs, 1, MPI_INT, 0, MPI_COMM_WORLD);
  }

  if (rank == 0)
    fclose(fp);

  // Calcula peso
  calculaPeso(nArmarios, nAssuntos, armarios);

  int control;
  do {
    control = 0;

    // Organização dos documentos nos armários com base na distância
    for (i = rank; i < nDocumentos; i += size) {
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

      if (doc->armarioPertence != armario->idA)
        control = 1;

      // Remove o documento da lista do armário anterior, se houver
      armarios[doc->armarioPertence].numDocs--;

      // SE ESTIVER NO MEIO
      if (doc->prox != NULL && doc->ant != NULL) {
        doc->ant->prox = doc->prox;
        doc->prox->ant = doc->ant;
      } else if (doc->ant != NULL) {
        doc->ant->prox = NULL;
      } else if (doc->prox != NULL && doc->ant == NULL) {
        doc->prox->ant = NULL;
        armarios[doc->armarioPertence].listDoc = doc->prox;
      } else
        armarios[doc->armarioPertence].listDoc = NULL;

      // Insere o documento na lista do armário selecionado
      if (armario->listDoc != NULL) {
        armario->listDoc->ant = doc;
        doc->prox = armario->listDoc;
        doc->ant = NULL;
        armario->listDoc = doc;
      } else {
        armario->listDoc = doc;
        doc->prox = NULL;
        doc->ant = NULL;
      }

      armario->numDocs++;            // Aumenta o número de documentos
      doc->armarioPertence = armario->idA;  // O documento recebe o ID do armário ao qual ele pertence
    }

    MPI_Allreduce(&control, &globalControl, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    MPI_Bcast(&globalControl, 1, MPI_INT, 0, MPI_COMM_WORLD);

    calculaPeso(nArmarios, nAssuntos, armarios);

    MPI_Bcast(&armarios[0].peso[0], nArmarios * nAssuntos, MPI_DOUBLE, 0, MPI_COMM_WORLD);

  } while (globalControl == 1);

  MPI_Gather(&documentos[rank * (nDocumentos / size)].idD, nDocumentos / size, MPI_INT, &documentos[0].idD, nDocumentos / size, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Gather(&documentos[rank * (nDocumentos / size)].armarioPertence, nDocumentos / size, MPI_INT, &documentos[0].armarioPertence, nDocumentos / size, MPI_INT, 0, MPI_COMM_WORLD);

  // Impressão dos armários e seus documentos
  if (rank == 0) {
    char *ponto = strtok(filename, ".");
    char aux[50];
    strcpy(aux, ponto);
    strcat(aux, ".out");
    fr = fopen(aux, "w+");

    for (i = 0; i < nDocumentos; i++) {
      fprintf(fr, "%d %d\n", documentos[i].idD, documentos[i].armarioPertence);
    }

    fclose(fr);

    // Libera a memória alocada
    for (i = 0; i < nArmarios; i++) {
      free(armarios[i].peso);
    }
    free(armarios);
    for (i = 0; i < nDocumentos; i++) {
      free(documentos[i].assunto);
    }
    free(documentos);
  }

  MPI_Finalize();

  if (rank == 0) {
    printf("Classificacao feita com sucesso!!! (^_^)\n");
  }

  return 0;
}

// Calcular os pesos dos armários
void calculaPeso(int nArmarios, int nAssuntos, Ar *armarios) {
  int indice = 0;
  double soma = 0;
  int k;
  int i;
  int j;
  Dc *docaux;

  for (i = 0; i < nArmarios; i++) {
    if (armarios[i].listDoc == NULL) {
      for (j = 0; j < nAssuntos; j++)
        armarios[i].peso[j] = 0;
    } else {
      for (k = 0; k < nAssuntos; k++) {
        indice = 0;
        soma = 0;
        docaux = armarios[i].listDoc;

        while (docaux != NULL) {
          soma += docaux->assunto[k];
          indice++;
          docaux = docaux->prox;
        }

        armarios[i].peso[k] = soma / indice;
      }
    }
  }
}

