import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity({ schema: 'app', name: 'viagens' })
export class ViagemEntity {
  @PrimaryGeneratedColumn({ type: 'integer', name: 'id_viagem' })
  idViagem!: number;

  @Column({ type: 'integer', name: 'id_veiculo' })
  idVeiculo!: number;

  @Column({ type: 'integer', name: 'id_motorista' })
  idMotorista!: number;

  @Column({ type: 'timestamp with time zone', name: 'data_inicio' })
  dataInicio!: Date;

  @Column({
    type: 'timestamp with time zone',
    name: 'data_fim',
    nullable: true,
  })
  dataFim!: Date | null;

  @Column({ type: 'integer', name: 'km_inicial' })
  kmInicial!: number;

  @Column({ type: 'integer', name: 'km_final', nullable: true })
  kmFinal!: number | null;

  @Column({ type: 'character', length: 1, name: 'status', default: 'A' })
  status!: string;

  @Column({ type: 'text', name: 'observacao', nullable: true })
  observacao!: string | null;

  @CreateDateColumn({ type: 'timestamp with time zone', name: 'criado_em' })
  criadoEm!: Date;

  @UpdateDateColumn({ type: 'timestamp with time zone', name: 'atualizado_em' })
  atualizadoEm!: Date;

  @Column({ type: 'text', name: 'usuario_atualizacao', nullable: true })
  usuarioAtualizacao!: string | null;

  @Column({ type: 'numeric', name: 'valor_frete', nullable: true, default: 0 })
  valorFrete!: string | number | null;

  @Column({ type: 'numeric', name: 'media', nullable: true, default: 0 })
  media!: string | number | null;

  @Column({
    type: 'numeric',
    name: 'total_despesas',
    nullable: true,
    default: 0,
  })
  totalDespesas!: string | number | null;

  @Column({
    type: 'numeric',
    name: 'total_abastecimentos',
    nullable: true,
    default: 0,
  })
  totalAbastecimentos!: string | number | null;

  @Column({ type: 'integer', name: 'total_km', nullable: true, default: 0 })
  totalKm!: number | null;

  @Column({ type: 'numeric', name: 'total_lucro', nullable: true, default: 0 })
  totalLucro!: string | number | null;

  @Column({ type: 'bigint', name: 'id_empresa' })
  idEmpresa!: string;
}
