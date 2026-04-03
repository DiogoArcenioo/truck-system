import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { APP_INTERCEPTOR } from '@nestjs/core';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ConfigService } from '@nestjs/config';
import { AuthModule } from './auth/auth.module';
import { AssinaturaAcessoInterceptor } from './auth/interceptors/assinatura-acesso.interceptor';
import { AbastecimentosModule } from './abastecimentos/abastecimentos.module';
import { CombustiveisModule } from './combustiveis/combustiveis.module';
import { CorVeiculoModule } from './cor-veiculo/cor-veiculo.module';
import { DashboardModule } from './dashboard/dashboard.module';
import { EngateDesengateModule } from './engate-desengate/engate-desengate.module';
import { FornecedorModule } from './fornecedor/fornecedor.module';
import { HealthController } from './health/health.controller';
import { MarcaVeiculoModule } from './marca-veiculo/marca-veiculo.module';
import { ModeloVeiculoModule } from './modelo-veiculo/modelo-veiculo.module';
import { MotoristasModule } from './motoristas/motoristas.module';
import { OrdemServicoModule } from './ordem-servico/ordem-servico.module';
import { ProdutoModule } from './produto/produto.module';
import { ProdutoReferenciasModule } from './produto-referencias/produto-referencias.module';
import { RequisicaoModule } from './requisicao/requisicao.module';
import { TiposVeiculoModule } from './tipos-veiculo/tipos-veiculo.module';
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
    CombustiveisModule,
    CorVeiculoModule,
    FornecedorModule,
    MarcaVeiculoModule,
    ModeloVeiculoModule,
    VeiculoModule,
    MotoristasModule,
    ViagensModule,
    ProdutoModule,
    ProdutoReferenciasModule,
    OrdemServicoModule,
    RequisicaoModule,
    TiposVeiculoModule,
    DashboardModule,
    EngateDesengateModule,
  ],
  controllers: [HealthController],
  providers: [
    {
      provide: APP_INTERCEPTOR,
      useClass: AssinaturaAcessoInterceptor,
    },
  ],
})
export class AppModule {}
