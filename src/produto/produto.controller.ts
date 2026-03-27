import {
  Body,
  Controller,
  Get,
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
    return this.produtoService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Put(':idProduto')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idProduto', ParseIntPipe) idProduto: number,
    @Body() dados: AtualizarProdutoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.produtoService.atualizar(
      usuario.idEmpresa,
      idProduto,
      dados,
      usuario,
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
}
