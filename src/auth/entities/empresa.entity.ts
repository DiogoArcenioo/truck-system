import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity({ schema: 'app', name: 'empresas' })
export class EmpresaEntity {
  @PrimaryGeneratedColumn({ type: 'bigint', name: 'id_empresa' })
  idEmpresa!: string;

  @Column({ type: 'character varying', length: 20, name: 'codigo' })
  codigo!: string;

  @Column({ type: 'character varying', length: 150, name: 'nome_fantasia' })
  nomeFantasia!: string;

  @Column({ type: 'character varying', length: 180, name: 'razao_social' })
  razaoSocial!: string;

  @Column({ type: 'character varying', length: 18, name: 'cnpj' })
  cnpj!: string;

  @Column({ type: 'character varying', length: 150, name: 'email_principal' })
  emailPrincipal!: string;

  @Column({ type: 'character varying', length: 25, name: 'telefone_principal' })
  telefonePrincipal!: string;

  @Column({ type: 'character varying', length: 25, name: 'whatsapp_principal' })
  whatsappPrincipal!: string;

  @Column({ type: 'boolean', name: 'ativo', default: true })
  ativo!: boolean;

  @Column({
    type: 'character varying',
    length: 20,
    name: 'status',
    default: 'ativo',
  })
  status!: string;

  @Column({
    type: 'character varying',
    length: 20,
    name: 'plano',
    default: 'basico',
  })
  plano!: string;

  @Column({ type: 'character varying', length: 100, name: 'slug' })
  slug!: string;

  @CreateDateColumn({ type: 'timestamp', name: 'criado_em' })
  criadoEm!: Date;

  @UpdateDateColumn({ type: 'timestamp', name: 'atualizado_em' })
  atualizadoEm!: Date;

  @Column({ type: 'text', name: 'usuario_atualizacao' })
  usuarioAtualizacao!: string;
}
