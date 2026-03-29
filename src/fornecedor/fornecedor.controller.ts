import {
  Body,
  Controller,
  Delete,
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
import { AtualizarFornecedorDto } from './dto/atualizar-fornecedor.dto';
import { CriarFornecedorDto } from './dto/criar-fornecedor.dto';
import { FiltroFornecedorDto } from './dto/filtro-fornecedor.dto';
import {
  AtualizarFornecedorContatoDto,
  CriarFornecedorContatoDto,
} from './dto/fornecedor-contato.dto';
import {
  AtualizarFornecedorEnderecoDto,
  CriarFornecedorEnderecoDto,
} from './dto/fornecedor-endereco.dto';
import { FornecedorService } from './fornecedor.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/fornecedor')
@UseGuards(JwtAuthGuard)
export class FornecedorController {
  constructor(private readonly fornecedorService: FornecedorService) {}

  @Get('opcoes')
  async listarOpcoes() {
    return this.fornecedorService.listarOpcoes();
  }

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.fornecedorService.listarTodos(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroFornecedorDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.fornecedorService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get(':idFornecedor')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idFornecedor', ParseIntPipe) idFornecedor: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.fornecedorService.buscarPorId(usuario.idEmpresa, idFornecedor);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarFornecedorDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.fornecedorService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Put(':idFornecedor')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idFornecedor', ParseIntPipe) idFornecedor: number,
    @Body() dados: AtualizarFornecedorDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.fornecedorService.atualizar(
      usuario.idEmpresa,
      idFornecedor,
      dados,
      usuario,
    );
  }

  @Delete(':idFornecedor')
  async remover(
    @Req() request: RequisicaoAutenticada,
    @Param('idFornecedor', ParseIntPipe) idFornecedor: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.fornecedorService.remover(usuario.idEmpresa, idFornecedor);
  }

  @Post(':idFornecedor/contatos')
  async adicionarContato(
    @Req() request: RequisicaoAutenticada,
    @Param('idFornecedor', ParseIntPipe) idFornecedor: number,
    @Body() dados: CriarFornecedorContatoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.fornecedorService.adicionarContato(
      usuario.idEmpresa,
      idFornecedor,
      dados,
      usuario,
    );
  }

  @Put(':idFornecedor/contatos/:idContato')
  async atualizarContato(
    @Req() request: RequisicaoAutenticada,
    @Param('idFornecedor', ParseIntPipe) idFornecedor: number,
    @Param('idContato', ParseIntPipe) idContato: number,
    @Body() dados: AtualizarFornecedorContatoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.fornecedorService.atualizarContato(
      usuario.idEmpresa,
      idFornecedor,
      idContato,
      dados,
      usuario,
    );
  }

  @Delete(':idFornecedor/contatos/:idContato')
  async removerContato(
    @Req() request: RequisicaoAutenticada,
    @Param('idFornecedor', ParseIntPipe) idFornecedor: number,
    @Param('idContato', ParseIntPipe) idContato: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.fornecedorService.removerContato(
      usuario.idEmpresa,
      idFornecedor,
      idContato,
    );
  }

  @Post(':idFornecedor/enderecos')
  async adicionarEndereco(
    @Req() request: RequisicaoAutenticada,
    @Param('idFornecedor', ParseIntPipe) idFornecedor: number,
    @Body() dados: CriarFornecedorEnderecoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.fornecedorService.adicionarEndereco(
      usuario.idEmpresa,
      idFornecedor,
      dados,
      usuario,
    );
  }

  @Put(':idFornecedor/enderecos/:idEndereco')
  async atualizarEndereco(
    @Req() request: RequisicaoAutenticada,
    @Param('idFornecedor', ParseIntPipe) idFornecedor: number,
    @Param('idEndereco', ParseIntPipe) idEndereco: number,
    @Body() dados: AtualizarFornecedorEnderecoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.fornecedorService.atualizarEndereco(
      usuario.idEmpresa,
      idFornecedor,
      idEndereco,
      dados,
      usuario,
    );
  }

  @Delete(':idFornecedor/enderecos/:idEndereco')
  async removerEndereco(
    @Req() request: RequisicaoAutenticada,
    @Param('idFornecedor', ParseIntPipe) idFornecedor: number,
    @Param('idEndereco', ParseIntPipe) idEndereco: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.fornecedorService.removerEndereco(
      usuario.idEmpresa,
      idFornecedor,
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
}
