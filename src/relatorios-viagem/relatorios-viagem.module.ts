import { Module } from '@nestjs/common';
import { AbastecimentosModule } from '../abastecimentos/abastecimentos.module';
import { DespesasModule } from '../despesas/despesas.module';
import { MultasModule } from '../multas/multas.module';
import { ViagensModule } from '../viagens/viagens.module';
import { RelatoriosViagemController } from './relatorios-viagem.controller';
import { RelatoriosViagemService } from './relatorios-viagem.service';

@Module({
  imports: [ViagensModule, AbastecimentosModule, DespesasModule, MultasModule],
  controllers: [RelatoriosViagemController],
  providers: [RelatoriosViagemService],
})
export class RelatoriosViagemModule {}
