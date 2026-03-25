import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AuthController } from './auth.controller';
import { AuthService } from './auth.service';
import { EmpresaEntity } from './entities/empresa.entity';
import { UsuarioEntity } from './entities/usuario.entity';

@Module({
  imports: [TypeOrmModule.forFeature([EmpresaEntity, UsuarioEntity])],
  controllers: [AuthController],
  providers: [AuthService],
})
export class AuthModule {}
