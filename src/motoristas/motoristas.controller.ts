import {
  Body,
  Controller,
  Delete,
  Get,
  HttpException,
  InternalServerErrorException,
  Logger,
  Param,
  ParseIntPipe,
  Post,
  Put,
  Query,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { QueryFailedError } from 'typeorm';
import { AtualizarMotoristaDto } from './dto/atualizar-motorista.dto';
import { CriarMotoristaDto } from './dto/criar-motorista.dto';
import { FiltroMotoristasDto } from './dto/filtro-motoristas.dto';
import {
  AtualizarMotoristaEnderecoDto,
  CriarMotoristaEnderecoDto,
} from './dto/motorista-endereco.dto';
import { MotoristasService } from './motoristas.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/motoristas')
@UseGuards(JwtAuthGuard)
export class MotoristasController {
  private readonly logger = new Logger(MotoristasController.name);

  constructor(private readonly motoristasService: MotoristasService) {}

  @Get('opcoes')
  async listarOpcoes() {
    return this.motoristasService.listarOpcoes();
  }

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.motoristasService.listarTodos(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroMotoristasDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.motoristasService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get(':idMotorista')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idMotorista', ParseIntPipe) idMotorista: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.motoristasService.buscarPorId(usuario.idEmpresa, idMotorista);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarMotoristaDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    try {
      return await this.motoristasService.cadastrar(
        usuario.idEmpresa,
        dados,
        usuario,
      );
    } catch (error) {
      if (error instanceof HttpException) {
        throw error;
      }

      const detalhe = this.descreverErro(error);
      this.logger.error(
        `Erro inesperado ao cadastrar motorista. empresa=${usuario.idEmpresa}`,
        error instanceof Error ? error.stack : undefined,
      );

      throw new InternalServerErrorException(
        `Falha inesperada ao cadastrar motorista. DEBUG: ${detalhe}`,
      );
    }
  }

  @Put(':idMotorista')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idMotorista', ParseIntPipe) idMotorista: number,
    @Body() dados: AtualizarMotoristaDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    try {
      return await this.motoristasService.atualizar(
        usuario.idEmpresa,
        idMotorista,
        dados,
        usuario,
      );
    } catch (error) {
      if (error instanceof HttpException) {
        throw error;
      }

      const detalhe = this.descreverErro(error);
      this.logger.error(
        `Erro inesperado ao atualizar motorista. empresa=${usuario.idEmpresa}, idMotorista=${idMotorista}`,
        error instanceof Error ? error.stack : undefined,
      );

      throw new InternalServerErrorException(
        `Falha inesperada ao atualizar motorista. DEBUG: ${detalhe}`,
      );
    }
  }

  @Post(':idMotorista/enderecos')
  async adicionarEndereco(
    @Req() request: RequisicaoAutenticada,
    @Param('idMotorista', ParseIntPipe) idMotorista: number,
    @Body() dados: CriarMotoristaEnderecoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.motoristasService.adicionarEndereco(
      usuario.idEmpresa,
      idMotorista,
      dados,
      usuario,
    );
  }

  @Put(':idMotorista/enderecos/:idEndereco')
  async atualizarEndereco(
    @Req() request: RequisicaoAutenticada,
    @Param('idMotorista', ParseIntPipe) idMotorista: number,
    @Param('idEndereco', ParseIntPipe) idEndereco: number,
    @Body() dados: AtualizarMotoristaEnderecoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.motoristasService.atualizarEndereco(
      usuario.idEmpresa,
      idMotorista,
      idEndereco,
      dados,
      usuario,
    );
  }

  @Delete(':idMotorista/enderecos/:idEndereco')
  async removerEndereco(
    @Req() request: RequisicaoAutenticada,
    @Param('idMotorista', ParseIntPipe) idMotorista: number,
    @Param('idEndereco', ParseIntPipe) idEndereco: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.motoristasService.removerEndereco(
      usuario.idEmpresa,
      idMotorista,
      idEndereco,
    );
  }

  private obterUsuarioAutenticado(
    request: RequisicaoAutenticada,
  ): JwtUsuarioPayload {
    if (!request.usuario) {
      throw new UnauthorizedException('Usuario nao autenticado.');
    }

    return request.usuario;
  }

  private descreverErro(error: unknown): string {
    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as {
        code?: string;
        detail?: string;
        message?: string;
      };
      return `QueryFailedError code=${erroPg.code ?? 'N/A'} detail=${erroPg.detail ?? erroPg.message ?? 'N/A'}`;
    }

    if (error instanceof Error) {
      return `${error.name}: ${error.message}`;
    }

    try {
      return JSON.stringify(error);
    } catch {
      return String(error);
    }
  }
}
