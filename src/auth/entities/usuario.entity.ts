import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity({ schema: 'app', name: 'usuarios' })
export class UsuarioEntity {
  @PrimaryGeneratedColumn({ type: 'bigint', name: 'id_usuario' })
  idUsuario!: string;

  @Column({ type: 'bigint', name: 'id_empresa' })
  idEmpresa!: string;

  @Column({ type: 'character varying', length: 150, name: 'nome' })
  nome!: string;

  @Column({ type: 'character varying', length: 150, name: 'email' })
  email!: string;

  @Column({ type: 'text', name: 'senha_hash' })
  senhaHash!: string;

  @Column({
    type: 'character varying',
    length: 20,
    name: 'perfil',
    default: 'ADM',
  })
  perfil!: string;

  @Column({ type: 'boolean', name: 'ativo', default: true })
  ativo!: boolean;

  @Column({
    type: 'timestamp with time zone',
    name: 'ultimo_login_em',
    nullable: true,
  })
  ultimoLoginEm!: Date | null;

  @CreateDateColumn({ type: 'timestamp with time zone', name: 'criado_em' })
  criadoEm!: Date;

  @UpdateDateColumn({ type: 'timestamp with time zone', name: 'atualizado_em' })
  atualizadoEm!: Date;

  @Column({ type: 'text', name: 'usuario_atualizacao' })
  usuarioAtualizacao!: string;
}
