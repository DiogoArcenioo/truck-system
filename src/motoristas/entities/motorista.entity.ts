import { Column, Entity, PrimaryColumn } from 'typeorm';

@Entity({ schema: 'app', name: 'motoristas' })
export class MotoristaEntity {
  @PrimaryColumn({ type: 'integer', name: 'id_motorista' })
  idMotorista!: number;

  @Column({ type: 'character varying', name: 'nome' })
  nome!: string;

  @Column({ type: 'character', length: 11, name: 'cpf' })
  cpf!: string;

  @Column({ type: 'character varying', name: 'cnh' })
  cnh!: string;

  @Column({ type: 'date', name: 'validade_cnh' })
  validadeCnh!: string;

  @Column({ type: 'character varying', name: 'categoria_cnh' })
  categoriaCnh!: string;

  @Column({ type: 'character', length: 1, name: 'status' })
  status!: string;

  @Column({ type: 'bigint', name: 'id_empresa' })
  idEmpresa!: string;
}
