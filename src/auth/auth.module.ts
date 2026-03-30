import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AuthController } from './auth.controller';
import { AuthService } from './auth.service';
import { EmpresaEntity } from './entities/empresa.entity';
import { UsuarioEntity } from './entities/usuario.entity';
import { InternalTokenGuard } from './guards/internal-token.guard';
import { LicencaController } from './licenca.controller';

@Module({
  imports: [TypeOrmModule.forFeature([EmpresaEntity, UsuarioEntity])],
  controllers: [AuthController, LicencaController],
  providers: [AuthService, InternalTokenGuard],
})
export class AuthModule {}
