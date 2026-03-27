import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ConfigService } from '@nestjs/config';
import { AuthModule } from './auth/auth.module';
import { AbastecimentosModule } from './abastecimentos/abastecimentos.module';
import { FornecedorModule } from './fornecedor/fornecedor.module';
import { HealthController } from './health/health.controller';
import { MotoristasModule } from './motoristas/motoristas.module';
import { OrdemServicoModule } from './ordem-servico/ordem-servico.module';
import { ProdutoModule } from './produto/produto.module';
import { VeiculoModule } from './veiculo/veiculo.module';
import { ViagensModule } from './viagens/viagens.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    TypeOrmModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        const host = configService.get<string>('DB_HOST');
        const database = configService.get<string>('DB_NAME');
        const username = configService.get<string>('DB_USER');
        const password = configService.get<string>('DB_PASSWORD');
        const port = Number(configService.get<string>('DB_PORT') ?? '5432');
        const sslRaw = (
          configService.get<string>('DB_SSL') ?? 'false'
        ).toLowerCase();
        const sslEnabled = sslRaw === 'true' || sslRaw === '1';

        if (!host || !database || !username || !password) {
          throw new Error(
            'Database env vars are missing. Set DB_HOST, DB_PORT, DB_NAME, DB_USER and DB_PASSWORD.',
          );
        }

        if (Number.isNaN(port)) {
          throw new Error('DB_PORT must be a valid number.');
        }

        return {
          type: 'postgres' as const,
          host,
          port,
          username,
          password,
          database,
          autoLoadEntities: true,
          synchronize: false,
          schema: 'app',
          ssl: sslEnabled ? { rejectUnauthorized: false } : false,
        };
      },
    }),
    AuthModule,
    AbastecimentosModule,
    FornecedorModule,
    VeiculoModule,
    MotoristasModule,
    ViagensModule,
    ProdutoModule,
    OrdemServicoModule,
  ],
  controllers: [HealthController],
})
export class AppModule {}
