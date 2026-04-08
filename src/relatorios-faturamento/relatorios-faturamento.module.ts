import { Module } from '@nestjs/common';
import { DespesasModule } from '../despesas/despesas.module';
import { ViagensModule } from '../viagens/viagens.module';
import { RelatoriosFaturamentoController } from './relatorios-faturamento.controller';
import { RelatoriosFaturamentoService } from './relatorios-faturamento.service';

@Module({
  imports: [ViagensModule, DespesasModule],
  controllers: [RelatoriosFaturamentoController],
  providers: [RelatoriosFaturamentoService],
})
export class RelatoriosFaturamentoModule {}
