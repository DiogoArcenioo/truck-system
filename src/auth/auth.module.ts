import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AuthController } from './auth.controller';
import { AuthService } from './auth.service';
import { EmpresaEntity } from './entities/empresa.entity';
import { UsuarioEntity } from './entities/usuario.entity';
import { InternalTokenGuard } from './guards/internal-token.guard';
import { LicencaController } from './licenca.controller';
import { PermissoesService } from './permissoes.service';
import { UsuariosController } from './usuarios.controller';

@Module({
  imports: [TypeOrmModule.forFeature([EmpresaEntity, UsuarioEntity])],
  controllers: [AuthController, LicencaController, UsuariosController],
  providers: [AuthService, InternalTokenGuard, PermissoesService],
  exports: [PermissoesService],
})
export class AuthModule {}
