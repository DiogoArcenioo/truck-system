import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity({ schema: 'app', name: 'manifestos' })
export class ManifestoEntity {
  @PrimaryGeneratedColumn({ type: 'bigint', name: 'id_manifesto' })
  idManifesto!: string;

  @Column({ type: 'bigint', name: 'id_empresa' })
  idEmpresa!: string;

  @Column({ type: 'integer', name: 'numero_manifesto' })
  numeroManifesto!: number;

  @Column({ type: 'integer', name: 'serie', default: 0 })
  serie!: number;

  @Column({ type: 'varchar', length: 44, name: 'chave_mdfe' })
  chaveMdfe!: string;

  @Column({
    type: 'varchar',
    length: 20,
    name: 'status_documento',
    default: 'AUTORIZADO',
  })
  statusDocumento!: string;

  @Column({ type: 'integer', name: 'cstat', nullable: true })
  cstat!: number | null;

  @Column({ type: 'text', name: 'motivo_status', nullable: true })
  motivoStatus!: string | null;

  @Column({ type: 'varchar', length: 30, name: 'protocolo', nullable: true })
  protocolo!: string | null;

  @Column({ type: 'timestamp with time zone', name: 'data_emissao' })
  dataEmissao!: Date;

  @Column({
    type: 'timestamp with time zone',
    name: 'data_autorizacao',
    nullable: true,
  })
  dataAutorizacao!: Date | null;

  @Column({
    type: 'timestamp with time zone',
    name: 'data_inicio_viagem',
    nullable: true,
  })
  dataInicioViagem!: Date | null;

  @Column({ type: 'char', length: 2, name: 'uf_inicio', nullable: true })
  ufInicio!: string | null;

  @Column({ type: 'char', length: 2, name: 'uf_fim', nullable: true })
  ufFim!: string | null;

  @Column({
    type: 'varchar',
    length: 120,
    name: 'municipio_carregamento',
    nullable: true,
  })
  municipioCarregamento!: string | null;

  @Column({ type: 'text', name: 'percurso_ufs', nullable: true })
  percursoUfs!: string | null;

  @Column({ type: 'varchar', length: 20, name: 'rntrc', nullable: true })
  rntrc!: string | null;

  @Column({ type: 'varchar', length: 12, name: 'placa_tracao', nullable: true })
  placaTracao!: string | null;

  @Column({ type: 'varchar', length: 12, name: 'placa_reboque1', nullable: true })
  placaReboque1!: string | null;

  @Column({ type: 'varchar', length: 12, name: 'placa_reboque2', nullable: true })
  placaReboque2!: string | null;

  @Column({ type: 'varchar', length: 12, name: 'placa_reboque3', nullable: true })
  placaReboque3!: string | null;

  @Column({
    type: 'varchar',
    length: 180,
    name: 'condutor_nome',
    nullable: true,
  })
  condutorNome!: string | null;

  @Column({ type: 'varchar', length: 11, name: 'condutor_cpf', nullable: true })
  condutorCpf!: string | null;

  @Column({ type: 'integer', name: 'quantidade_cte', default: 0 })
  quantidadeCte!: number;

  @Column({ type: 'text', name: 'chaves_cte', nullable: true })
  chavesCte!: string | null;

  @Column({
    type: 'numeric',
    precision: 14,
    scale: 2,
    name: 'valor_carga',
    default: 0,
  })
  valorCarga!: string | number;

  @Column({
    type: 'numeric',
    precision: 14,
    scale: 3,
    name: 'quantidade_carga',
    default: 0,
  })
  quantidadeCarga!: string | number;

  @Column({
    type: 'varchar',
    length: 180,
    name: 'produto_predominante',
    nullable: true,
  })
  produtoPredominante!: string | null;

  @Column({
    type: 'varchar',
    length: 180,
    name: 'seguradora_nome',
    nullable: true,
  })
  seguradoraNome!: string | null;

  @Column({
    type: 'varchar',
    length: 80,
    name: 'apolice_numero',
    nullable: true,
  })
  apoliceNumero!: string | null;

  @Column({
    type: 'varchar',
    length: 80,
    name: 'averbacao_numero',
    nullable: true,
  })
  averbacaoNumero!: string | null;

  @Column({ type: 'text', name: 'qr_code_url', nullable: true })
  qrCodeUrl!: string | null;

  @Column({ type: 'text', name: 'observacao', nullable: true })
  observacao!: string | null;

  @Column({ type: 'boolean', name: 'ativo', default: true })
  ativo!: boolean;

  @Column({ type: 'text', name: 'usuario_atualizacao', default: 'SISTEMA' })
  usuarioAtualizacao!: string;

  @CreateDateColumn({ type: 'timestamp with time zone', name: 'criado_em' })
  criadoEm!: Date;

  @UpdateDateColumn({ type: 'timestamp with time zone', name: 'atualizado_em' })
  atualizadoEm!: Date;
}
