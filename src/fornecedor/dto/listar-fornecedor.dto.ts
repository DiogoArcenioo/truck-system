export class ListarFornecedorContatoDto {
  idContato!: number;
  idFornecedor!: number;
  nomeContato!: string;
  telefone!: string | null;
  celular!: string | null;
  email!: string | null;
  principal!: boolean;
  usuarioAtualizacao!: string | null;
  criadoEm!: string | null;
  atualizadoEm!: string | null;
}

export class ListarFornecedorEnderecoDto {
  idEndereco!: number;
  idFornecedor!: number;
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

export class ListarFornecedorDto {
  idFornecedor!: number;
  tipoPessoa!: string;
  razaoSocial!: string | null;
  nomeFantasia!: string | null;
  nomePessoa!: string | null;
  cpf!: string | null;
  cnpj!: string | null;
  inscricaoEstadual!: string | null;
  inscricaoMunicipal!: string | null;
  observacoes!: string | null;
  ativo!: boolean;
  status!: string;
  nome!: string;
  dataCadastro!: string | null;
  usuarioAtualizacao!: string | null;
  atualizadoEm!: string | null;
  qtdContatos!: number;
  qtdEnderecos!: number;
  contatos?: ListarFornecedorContatoDto[];
  enderecos?: ListarFornecedorEnderecoDto[];
}
