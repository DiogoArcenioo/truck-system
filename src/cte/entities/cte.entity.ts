import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity({ schema: 'app', name: 'ctes' })
export class CteEntity {
  @PrimaryGeneratedColumn({ type: 'bigint', name: 'id_cte' })
  idCte!: string;

  @Column({ type: 'bigint', name: 'id_empresa' })
  idEmpresa!: string;

  @Column({ type: 'integer', name: 'numero_cte' })
  numeroCte!: number;

  @Column({ type: 'integer', name: 'serie', default: 0 })
  serie!: number;

  @Column({ type: 'varchar', length: 44, name: 'chave_cte' })
  chaveCte!: string;

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

  @Column({ type: 'varchar', length: 4, name: 'cfop', nullable: true })
  cfop!: string | null;

  @Column({
    type: 'varchar',
    length: 180,
    name: 'natureza_operacao',
    nullable: true,
  })
  naturezaOperacao!: string | null;

  @Column({
    type: 'varchar',
    length: 120,
    name: 'municipio_inicio',
    nullable: true,
  })
  municipioInicio!: string | null;

  @Column({ type: 'char', length: 2, name: 'uf_inicio', nullable: true })
  ufInicio!: string | null;

  @Column({
    type: 'varchar',
    length: 120,
    name: 'municipio_fim',
    nullable: true,
  })
  municipioFim!: string | null;

  @Column({ type: 'char', length: 2, name: 'uf_fim', nullable: true })
  ufFim!: string | null;

  @Column({
    type: 'varchar',
    length: 180,
    name: 'remetente_nome',
    nullable: true,
  })
  remetenteNome!: string | null;

  @Column({
    type: 'varchar',
    length: 14,
    name: 'remetente_cnpj',
    nullable: true,
  })
  remetenteCnpj!: string | null;

  @Column({
    type: 'varchar',
    length: 180,
    name: 'destinatario_nome',
    nullable: true,
  })
  destinatarioNome!: string | null;

  @Column({
    type: 'varchar',
    length: 14,
    name: 'destinatario_cnpj',
    nullable: true,
  })
  destinatarioCnpj!: string | null;

  @Column({ type: 'varchar', length: 180, name: 'tomador_nome', nullable: true })
  tomadorNome!: string | null;

  @Column({ type: 'varchar', length: 14, name: 'tomador_cnpj', nullable: true })
  tomadorCnpj!: string | null;

  @Column({
    type: 'varchar',
    length: 180,
    name: 'motorista_nome',
    nullable: true,
  })
  motoristaNome!: string | null;

  @Column({ type: 'varchar', length: 11, name: 'motorista_cpf', nullable: true })
  motoristaCpf!: string | null;

  @Column({ type: 'varchar', length: 12, name: 'placa_tracao', nullable: true })
  placaTracao!: string | null;

  @Column({ type: 'varchar', length: 12, name: 'placa_reboque1', nullable: true })
  placaReboque1!: string | null;

  @Column({ type: 'varchar', length: 12, name: 'placa_reboque2', nullable: true })
  placaReboque2!: string | null;

  @Column({ type: 'varchar', length: 12, name: 'placa_reboque3', nullable: true })
  placaReboque3!: string | null;

  @Column({
    type: 'numeric',
    name: 'valor_total_prestacao',
    precision: 14,
    scale: 2,
    default: 0,
  })
  valorTotalPrestacao!: string | number;

  @Column({
    type: 'numeric',
    name: 'valor_receber',
    precision: 14,
    scale: 2,
    default: 0,
  })
  valorReceber!: string | number;

  @Column({
    type: 'numeric',
    name: 'valor_icms',
    precision: 14,
    scale: 2,
    default: 0,
  })
  valorIcms!: string | number;

  @Column({
    type: 'numeric',
    name: 'valor_carga',
    precision: 14,
    scale: 2,
    default: 0,
  })
  valorCarga!: string | number;

  @Column({
    type: 'numeric',
    name: 'peso_bruto',
    precision: 14,
    scale: 3,
    default: 0,
  })
  pesoBruto!: string | number;

  @Column({
    type: 'numeric',
    name: 'quantidade_volumes',
    precision: 14,
    scale: 3,
    default: 0,
  })
  quantidadeVolumes!: string | number;

  @Column({ type: 'varchar', length: 44, name: 'chave_mdfe', nullable: true })
  chaveMdfe!: string | null;

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
