import { Module } from '@nestjs/common';
import { RelatoriosFaturamentoModule } from '../relatorios-faturamento/relatorios-faturamento.module';
import { RelatoriosResultadoController } from './relatorios-resultado.controller';
import { RelatoriosResultadoService } from './relatorios-resultado.service';

@Module({
  imports: [RelatoriosFaturamentoModule],
  controllers: [RelatoriosResultadoController],
  providers: [RelatoriosResultadoService],
})
export class RelatoriosResultadoModule {}
