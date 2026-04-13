import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { ManifestoController } from './manifesto.controller';
import { ManifestoService } from './manifesto.service';
import { ManifestoEntity } from './entities/manifesto.entity';

@Module({
  imports: [TypeOrmModule.forFeature([ManifestoEntity])],
  controllers: [ManifestoController],
  providers: [ManifestoService, JwtAuthGuard],
  exports: [ManifestoService],
})
export class ManifestoModule {}
