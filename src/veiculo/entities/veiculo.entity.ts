import { Column, Entity, PrimaryColumn } from 'typeorm';

@Entity({ schema: 'app', name: 'veiculo' })
export class VeiculoEntity {
  @PrimaryColumn({ type: 'integer', name: 'id_veiculo' })
  idVeiculo!: number;

  @Column({ type: 'character varying', name: 'placa' })
  placa!: string;

  @Column({ type: 'character varying', name: 'placa2', nullable: true })
  placa2!: string | null;

  @Column({ type: 'character varying', name: 'placa3', nullable: true })
  placa3!: string | null;

  @Column({ type: 'character varying', name: 'placa4', nullable: true })
  placa4!: string | null;

  @Column({ type: 'integer', name: 'id_motorista_atual', nullable: true })
  idMotoristaAtual!: number | null;

  @Column({ type: 'integer', name: 'km_atual', nullable: true })
  kmAtual!: number | null;

  @Column({ type: 'integer', name: 'ano_fabricacao', nullable: true })
  anoFabricacao!: number | null;

  @Column({ type: 'integer', name: 'ano_modelo', nullable: true })
  anoModelo!: number | null;

  @Column({ type: 'date', name: 'data_vencimento', nullable: true })
  dataVencimento!: string | null;

  @Column({ type: 'bigint', name: 'id_empresa' })
  idEmpresa!: string;
}
