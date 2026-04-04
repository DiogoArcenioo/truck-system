import { Column, Entity, PrimaryGeneratedColumn } from 'typeorm';

@Entity({ schema: 'app', name: 'motoristas' })
export class MotoristaEntity {
  @PrimaryGeneratedColumn({ type: 'integer', name: 'id_motorista' })
  idMotorista!: number;

  @Column({ type: 'character varying', name: 'nome' })
  nome!: string;

  @Column({ type: 'character', length: 11, name: 'cpf' })
  cpf!: string;

  @Column({ type: 'character varying', name: 'cnh' })
  cnh!: string;

  @Column({ type: 'date', name: 'data_nascimento', nullable: true })
  dataNascimento!: string | null;

  @Column({ type: 'character varying', length: 150, name: 'email', nullable: true })
  email!: string | null;

  @Column({ type: 'character varying', length: 20, name: 'telefone1', nullable: true })
  telefone1!: string | null;

  @Column({ type: 'character varying', length: 20, name: 'telefone2', nullable: true })
  telefone2!: string | null;

  @Column({ type: 'date', name: 'validade_cnh' })
  validadeCnh!: string;

  @Column({ type: 'character varying', name: 'categoria_cnh' })
  categoriaCnh!: string;

  @Column({ type: 'date', name: 'data_admissao', nullable: true })
  dataAdmissao!: string | null;

  @Column({ type: 'date', name: 'data_demissao', nullable: true })
  dataDemissao!: string | null;

  @Column({ type: 'character varying', length: 20, name: 'tipo_contrato', nullable: true })
  tipoContrato!: string | null;

  @Column({ type: 'character', length: 1, name: 'status' })
  status!: string;

  @Column({ type: 'bigint', name: 'id_empresa' })
  idEmpresa!: string;
}
