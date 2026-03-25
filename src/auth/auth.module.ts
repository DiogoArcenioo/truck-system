import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AuthController } from './auth.controller';
import { AuthService } from './auth.service';
import { EmpresaEntity } from './entities/empresa.entity';

@Module({
  imports: [TypeOrmModule.forFeature([EmpresaEntity])],
  controllers: [AuthController],
  providers: [AuthService],
})
export class AuthModule {}
