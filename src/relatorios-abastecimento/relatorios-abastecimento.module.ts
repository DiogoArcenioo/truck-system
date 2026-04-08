import { Module } from '@nestjs/common';
import { AbastecimentosModule } from '../abastecimentos/abastecimentos.module';
import { ViagensModule } from '../viagens/viagens.module';
import { RelatoriosAbastecimentoController } from './relatorios-abastecimento.controller';
import { RelatoriosAbastecimentoService } from './relatorios-abastecimento.service';

@Module({
  imports: [AbastecimentosModule, ViagensModule],
  controllers: [RelatoriosAbastecimentoController],
  providers: [RelatoriosAbastecimentoService],
})
export class RelatoriosAbastecimentoModule {}
