import {
  Body,
  Controller,
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
import { AtualizarProdutoDto } from './dto/atualizar-produto.dto';
import { CriarProdutoDto } from './dto/criar-produto.dto';
import { FiltroProdutosDto } from './dto/filtro-produtos.dto';
import { ProdutoService } from './produto.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/produto')
@UseGuards(JwtAuthGuard)
export class ProdutoController {
  private readonly logger = new Logger(ProdutoController.name);

  constructor(private readonly produtoService: ProdutoService) {}

  @Get('opcoes')
  async listarOpcoes() {
    return this.produtoService.listarOpcoes();
  }

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.produtoService.listarTodos(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroProdutosDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.produtoService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get(':idProduto')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idProduto', ParseIntPipe) idProduto: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.produtoService.buscarPorId(usuario.idEmpresa, idProduto);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarProdutoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    try {
      return await this.produtoService.cadastrar(usuario.idEmpresa, dados, usuario);
    } catch (error) {
      if (error instanceof HttpException) {
        throw error;
      }

      const detalhe = this.descreverErro(error);
      this.logger.error(
        `Erro inesperado ao cadastrar produto. empresa=${usuario.idEmpresa}`,
        error instanceof Error ? error.stack : undefined,
      );

      throw new InternalServerErrorException(
        `Falha inesperada ao cadastrar produto. DEBUG: ${detalhe}`,
      );
    }
  }

  @Put(':idProduto')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idProduto', ParseIntPipe) idProduto: number,
    @Body() dados: AtualizarProdutoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    try {
      return await this.produtoService.atualizar(
        usuario.idEmpresa,
        idProduto,
        dados,
        usuario,
      );
    } catch (error) {
      if (error instanceof HttpException) {
        throw error;
      }

      const detalhe = this.descreverErro(error);
      this.logger.error(
        `Erro inesperado ao atualizar produto. empresa=${usuario.idEmpresa}, idProduto=${idProduto}`,
        error instanceof Error ? error.stack : undefined,
      );

      throw new InternalServerErrorException(
        `Falha inesperada ao atualizar produto. DEBUG: ${detalhe}`,
      );
    }
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
