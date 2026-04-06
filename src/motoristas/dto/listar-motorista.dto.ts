export class ListarMotoristaEnderecoDto {
  idEndereco!: number;
  idMotorista!: number;
  logradouro!: string;
  numero!: string | null;
  complemento!: string | null;
  bairro!: string | null;
  cidade!: string | null;
  estado!: string | null;
  cep!: string | null;
  principal!: boolean;
  usuarioAtualizacao!: string | null;
  criadoEm!: string | null;
  atualizadoEm!: string | null;
}

export class ListarMotoristaDto {
  idMotorista!: number;
  nome!: string;
  cpf!: string;
  cnh!: string;
  dataNascimento!: string;
  email!: string;
  telefone1!: string;
  telefone2!: string;
  categoriaCnh!: string;
  validadeCnh!: string;
  dataAdmissao!: string;
  dataDemissao!: string;
  tipoContrato!: string;
  status!: string;
  statusDescricao!: string;
  qtdEnderecos?: number;
  enderecos?: ListarMotoristaEnderecoDto[];
}
