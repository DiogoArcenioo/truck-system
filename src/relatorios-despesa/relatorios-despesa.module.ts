import { Module } from '@nestjs/common';
import { DespesasModule } from '../despesas/despesas.module';
import { RelatoriosDespesaController } from './relatorios-despesa.controller';
import { RelatoriosDespesaService } from './relatorios-despesa.service';

@Module({
  imports: [DespesasModule],
  controllers: [RelatoriosDespesaController],
  providers: [RelatoriosDespesaService],
})
export class RelatoriosDespesaModule {}
