import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { NotasFiscaisAbastecimentoController } from './notas-fiscais-abastecimento.controller';
import { NotasFiscaisAbastecimentoService } from './notas-fiscais-abastecimento.service';

@Module({
  controllers: [NotasFiscaisAbastecimentoController],
  providers: [NotasFiscaisAbastecimentoService, JwtAuthGuard],
  exports: [NotasFiscaisAbastecimentoService],
})
export class NotasFiscaisAbastecimentoModule {}
