import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  ParseIntPipe,
  Put,
  Post,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { CriarNotaFiscalAbastecimentoDto } from './dto/criar-nota-fiscal-abastecimento.dto';
import { ImportarXmlNotaFiscalAbastecimentoDto } from './dto/importar-xml-nota-fiscal-abastecimento.dto';
import { NotasFiscaisAbastecimentoService } from './notas-fiscais-abastecimento.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/notas-fiscais-abastecimento')
@UseGuards(JwtAuthGuard)
export class NotasFiscaisAbastecimentoController {
  constructor(
    private readonly notasFiscaisAbastecimentoService: NotasFiscaisAbastecimentoService,
  ) {}

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.notasFiscaisAbastecimentoService.listarTodos(usuario.idEmpresa);
  }

  @Get(':idNotaFiscal')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idNotaFiscal', ParseIntPipe) idNotaFiscal: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.notasFiscaisAbastecimentoService.buscarPorId(
      usuario.idEmpresa,
      idNotaFiscal,
    );
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarNotaFiscalAbastecimentoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.notasFiscaisAbastecimentoService.cadastrar(
      usuario.idEmpresa,
      dados,
      usuario,
    );
  }

  @Put(':idNotaFiscal')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idNotaFiscal', ParseIntPipe) idNotaFiscal: number,
    @Body() dados: CriarNotaFiscalAbastecimentoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.notasFiscaisAbastecimentoService.atualizar(
      usuario.idEmpresa,
      idNotaFiscal,
      dados,
      usuario,
    );
  }

  @Post('importar-xml')
  async importarXml(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: ImportarXmlNotaFiscalAbastecimentoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.notasFiscaisAbastecimentoService.importarXml(
      usuario.idEmpresa,
      dados.xml,
    );
  }

  @Post(':idNotaFiscal/efetivar')
  async efetivar(
    @Req() request: RequisicaoAutenticada,
    @Param('idNotaFiscal', ParseIntPipe) idNotaFiscal: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.notasFiscaisAbastecimentoService.efetivar(
      usuario.idEmpresa,
      idNotaFiscal,
      usuario,
    );
  }

  @Delete(':idNotaFiscal')
  async remover(
    @Req() request: RequisicaoAutenticada,
    @Param('idNotaFiscal', ParseIntPipe) idNotaFiscal: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.notasFiscaisAbastecimentoService.remover(
      usuario.idEmpresa,
      idNotaFiscal,
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
