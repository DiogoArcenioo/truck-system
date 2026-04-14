import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import {
  EntityManager,
  QueryFailedError,
  Repository,
  SelectQueryBuilder,
} from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { AtualizarCteDto } from './dto/atualizar-cte.dto';
import { CriarCteDto } from './dto/criar-cte.dto';
import { FiltroCteDto } from './dto/filtro-cte.dto';
import { CteEntity } from './entities/cte.entity';

type CteNormalizado = {
  idCte: number;
  idEmpresa: number;
  numeroCte: number;
  serie: number;
  chaveCte: string;
  statusDocumento: string;
  cstat: number | null;
  motivoStatus: string | null;
  protocolo: string | null;
  dataEmissao: Date;
  dataAutorizacao: Date | null;
  cfop: string | null;
  naturezaOperacao: string | null;
  municipioInicio: string | null;
  ufInicio: string | null;
  municipioFim: string | null;
  ufFim: string | null;
  remetenteNome: string | null;
  remetenteCnpj: string | null;
  destinatarioNome: string | null;
  destinatarioCnpj: string | null;
  tomadorNome: string | null;
  tomadorCnpj: string | null;
  motoristaNome: string | null;
  motoristaCpf: string | null;
  placaTracao: string | null;
  placaReboque1: string | null;
  placaReboque2: string | null;
  placaReboque3: string | null;
  valorTotalPrestacao: number;
  valorReceber: number;
  valorIcms: number;
  valorCarga: number;
  pesoBruto: number;
  quantidadeVolumes: number;
  chaveMdfe: string | null;
  qrCodeUrl: string | null;
  observacao: string | null;
  ativo: boolean;
  usuarioAtualizacao: string;
  criadoEm: Date;
  atualizadoEm: Date;
};

type OrdenacaoCampo =
  | 'id_cte'
  | 'numero_cte'
  | 'serie'
  | 'data_emissao'
  | 'data_autorizacao'
  | 'atualizado_em';

@Injectable()
export class CteService {
  private readonly logger = new Logger(CteService.name);

  private readonly colunasOrdenacao: Record<OrdenacaoCampo, string> = {
    id_cte: 'cte.idCte',
    numero_cte: 'cte.numeroCte',
    serie: 'cte.serie',
    data_emissao: 'cte.dataEmissao',
    data_autorizacao: 'cte.dataAutorizacao',
    atualizado_em: 'cte.atualizadoEm',
  };

  constructor(
    @InjectRepository(CteEntity)
    private readonly cteRepository: Repository<CteEntity>,
  ) {}

  async listarTodas(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (cteRepository) => {
      const itens = await cteRepository.find({
        where: { idEmpresa: String(idEmpresa), ativo: true },
        order: { dataEmissao: 'DESC', idCte: 'DESC' },
      });

      return {
        sucesso: true,
        total: itens.length,
        ctes: itens.map((item) => this.mapearCte(item)),
      };
    });
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroCteDto) {
    this.validarIntervaloData(filtro.dataEmissaoDe, filtro.dataEmissaoAte);

    return this.executarComRls(idEmpresa, async (cteRepository) => {
      const pagina = filtro.pagina ?? 1;
      const limite = filtro.limite ?? 30;
      const offset = (pagina - 1) * limite;

      const query = cteRepository
        .createQueryBuilder('cte')
        .where('cte.idEmpresa = :idEmpresa', { idEmpresa: String(idEmpresa) });

      if (!filtro.incluirInativos) {
        query.andWhere('cte.ativo = true');
      }

      if (filtro.idCte !== undefined) {
        query.andWhere('cte.idCte = :idCte', { idCte: filtro.idCte });
      }

      if (filtro.numeroCte !== undefined) {
        query.andWhere('cte.numeroCte = :numeroCte', {
          numeroCte: filtro.numeroCte,
        });
      }

      if (filtro.serie !== undefined) {
        query.andWhere('cte.serie = :serie', { serie: filtro.serie });
      }

      if (filtro.chaveCte?.trim()) {
        query.andWhere('cte.chaveCte ILIKE :chaveCte', {
          chaveCte: `%${filtro.chaveCte.trim()}%`,
        });
      }

      if (filtro.statusDocumento) {
        query.andWhere('cte.statusDocumento = :statusDocumento', {
          statusDocumento: filtro.statusDocumento,
        });
      }

      if (filtro.texto?.trim()) {
        const texto = `%${filtro.texto.trim()}%`;
        query.andWhere(
          `
          (
            CAST(cte.numeroCte AS TEXT) ILIKE :texto
            OR cte.chaveCte ILIKE :texto
            OR COALESCE(cte.motoristaNome, '') ILIKE :texto
            OR COALESCE(cte.placaTracao, '') ILIKE :texto
            OR COALESCE(cte.destinatarioNome, '') ILIKE :texto
            OR COALESCE(cte.remetenteNome, '') ILIKE :texto
          )
          `,
          { texto },
        );
      }

      if (filtro.dataEmissaoDe) {
        query.andWhere('cte.dataEmissao >= :dataEmissaoDe', {
          dataEmissaoDe: filtro.dataEmissaoDe,
        });
      }

      if (filtro.dataEmissaoAte) {
        query.andWhere('cte.dataEmissao <= :dataEmissaoAte', {
          dataEmissaoAte: filtro.dataEmissaoAte,
        });
      }

      this.aplicarOrdenacao(query, filtro);
      query.skip(offset).take(limite);

      const [itens, total] = await query.getManyAndCount();

      return {
        sucesso: true,
        pagina,
        limite,
        total,
        paginas: total > 0 ? Math.ceil(total / limite) : 0,
        ctes: itens.map((item) => this.mapearCte(item)),
      };
    });
  }

  async buscarPorId(idEmpresa: number, idCte: number) {
    return this.executarComRls(idEmpresa, async (cteRepository) => {
      const item = await this.buscarPorIdOuFalhar(cteRepository, idEmpresa, idCte);
      return { sucesso: true, cte: this.mapearCte(item) };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarCteDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const payload = this.normalizarCriacao(dados, usuarioJwt);

    try {
      return this.executarComRls(idEmpresa, async (cteRepository) => {
        const salvo = await cteRepository.save(
          cteRepository.create({
            ...payload,
            idEmpresa: String(idEmpresa),
          }),
        );

        return {
          sucesso: true,
          mensagem: 'CT-e cadastrado com sucesso.',
          cte: this.mapearCte(salvo),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar');
    }
  }

  async atualizar(
    idEmpresa: number,
    idCte: number,
    dados: AtualizarCteDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const payload = this.normalizarAtualizacao(dados, usuarioJwt);

    try {
      return this.executarComRls(idEmpresa, async (cteRepository) => {
        await this.buscarPorIdOuFalhar(cteRepository, idEmpresa, idCte);

        await cteRepository.update(
          { idCte: String(idCte), idEmpresa: String(idEmpresa) },
          payload,
        );

        const atualizado = await this.buscarPorIdOuFalhar(
          cteRepository,
          idEmpresa,
          idCte,
        );

        return {
          sucesso: true,
          mensagem: 'CT-e atualizado com sucesso.',
          cte: this.mapearCte(atualizado),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  async remover(idEmpresa: number, idCte: number, usuarioJwt: JwtUsuarioPayload) {
    return this.executarComRls(idEmpresa, async (cteRepository) => {
      const item = await this.buscarPorIdOuFalhar(cteRepository, idEmpresa, idCte);

      if (!item.ativo) {
        throw new BadRequestException('CT-e ja esta inativo.');
      }

      await cteRepository.update(
        { idCte: String(idCte), idEmpresa: String(idEmpresa) },
        {
          ativo: false,
          usuarioAtualizacao: this.normalizarUsuario(usuarioJwt),
          atualizadoEm: new Date(),
        },
      );

      return {
        sucesso: true,
        mensagem: 'CT-e inativado com sucesso.',
        idCte,
      };
    });
  }

  private async buscarPorIdOuFalhar(
    cteRepository: Repository<CteEntity>,
    idEmpresa: number,
    idCte: number,
  ) {
    const item = await cteRepository.findOne({
      where: { idCte: String(idCte), idEmpresa: String(idEmpresa) },
    });

    if (!item) {
      throw new NotFoundException('CT-e nao encontrado para a empresa logada.');
    }

    return item;
  }

  private aplicarOrdenacao(
    query: SelectQueryBuilder<CteEntity>,
    filtro: FiltroCteDto,
  ) {
    const campo = (filtro.ordenarPor ?? 'data_emissao') as OrdenacaoCampo;
    const ordem = filtro.ordem ?? 'DESC';
    const coluna = this.colunasOrdenacao[campo] ?? this.colunasOrdenacao.data_emissao;
    query.orderBy(coluna, ordem).addOrderBy('cte.idCte', 'DESC');
  }

  private normalizarCriacao(dados: CriarCteDto, usuarioJwt: JwtUsuarioPayload) {
    const dataEmissao = new Date(dados.dataEmissao);
    const dataAutorizacao = dados.dataAutorizacao
      ? new Date(dados.dataAutorizacao)
      : null;
    this.validarData(dataEmissao, 'dataEmissao');
    if (dataAutorizacao) this.validarData(dataAutorizacao, 'dataAutorizacao');

    return {
      numeroCte: dados.numeroCte,
      serie: dados.serie,
      chaveCte: dados.chaveCte.trim(),
      statusDocumento: (dados.statusDocumento ?? 'AUTORIZADO').toUpperCase(),
      cstat: dados.cstat ?? null,
      motivoStatus: this.textoOuNulo(dados.motivoStatus),
      protocolo: this.textoOuNulo(dados.protocolo),
      dataEmissao,
      dataAutorizacao,
      cfop: this.textoOuNulo(dados.cfop),
      naturezaOperacao: this.textoOuNulo(dados.naturezaOperacao),
      municipioInicio: this.textoOuNulo(dados.municipioInicio),
      ufInicio: this.normalizarUf(dados.ufInicio),
      municipioFim: this.textoOuNulo(dados.municipioFim),
      ufFim: this.normalizarUf(dados.ufFim),
      remetenteNome: this.textoOuNulo(dados.remetenteNome),
      remetenteCnpj: this.textoOuNulo(dados.remetenteCnpj),
      destinatarioNome: this.textoOuNulo(dados.destinatarioNome),
      destinatarioCnpj: this.textoOuNulo(dados.destinatarioCnpj),
      tomadorNome: this.textoOuNulo(dados.tomadorNome),
      tomadorCnpj: this.textoOuNulo(dados.tomadorCnpj),
      motoristaNome: this.textoOuNulo(dados.motoristaNome),
      motoristaCpf: this.textoOuNulo(dados.motoristaCpf),
      placaTracao: this.normalizarPlaca(dados.placaTracao),
      placaReboque1: this.normalizarPlaca(dados.placaReboque1),
      placaReboque2: this.normalizarPlaca(dados.placaReboque2),
      placaReboque3: this.normalizarPlaca(dados.placaReboque3),
      valorTotalPrestacao: dados.valorTotalPrestacao ?? 0,
      valorReceber: dados.valorReceber ?? 0,
      valorIcms: dados.valorIcms ?? 0,
      valorCarga: dados.valorCarga ?? 0,
      pesoBruto: dados.pesoBruto ?? 0,
      quantidadeVolumes: dados.quantidadeVolumes ?? 0,
      chaveMdfe: this.textoOuNulo(dados.chaveMdfe),
      qrCodeUrl: this.textoOuNulo(dados.qrCodeUrl),
      observacao: this.textoOuNulo(dados.observacao),
      ativo: true,
      usuarioAtualizacao:
        this.textoOuNulo(dados.usuarioAtualizacao) ??
        this.normalizarUsuario(usuarioJwt),
    };
  }

  private normalizarAtualizacao(
    dados: AtualizarCteDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const atualizado: Partial<CteEntity> = {
      usuarioAtualizacao:
        this.textoOuNulo(dados.usuarioAtualizacao) ??
        this.normalizarUsuario(usuarioJwt),
    };

    if (dados.numeroCte !== undefined) atualizado.numeroCte = dados.numeroCte;
    if (dados.serie !== undefined) atualizado.serie = dados.serie;
    if (dados.chaveCte !== undefined) atualizado.chaveCte = dados.chaveCte.trim();
    if (dados.statusDocumento !== undefined) {
      atualizado.statusDocumento = dados.statusDocumento.toUpperCase();
    }
    if (dados.cstat !== undefined) atualizado.cstat = dados.cstat;
    if (dados.motivoStatus !== undefined) {
      atualizado.motivoStatus = this.textoOuNulo(dados.motivoStatus);
    }
    if (dados.protocolo !== undefined) {
      atualizado.protocolo = this.textoOuNulo(dados.protocolo);
    }
    if (dados.dataEmissao !== undefined) {
      const data = new Date(dados.dataEmissao);
      this.validarData(data, 'dataEmissao');
      atualizado.dataEmissao = data;
    }
    if (dados.dataAutorizacao !== undefined) {
      if (dados.dataAutorizacao === null) {
        atualizado.dataAutorizacao = null;
      } else {
        const data = new Date(dados.dataAutorizacao);
        this.validarData(data, 'dataAutorizacao');
        atualizado.dataAutorizacao = data;
      }
    }
    if (dados.cfop !== undefined) atualizado.cfop = this.textoOuNulo(dados.cfop);
    if (dados.naturezaOperacao !== undefined) {
      atualizado.naturezaOperacao = this.textoOuNulo(dados.naturezaOperacao);
    }
    if (dados.municipioInicio !== undefined) {
      atualizado.municipioInicio = this.textoOuNulo(dados.municipioInicio);
    }
    if (dados.ufInicio !== undefined) atualizado.ufInicio = this.normalizarUf(dados.ufInicio);
    if (dados.municipioFim !== undefined) {
      atualizado.municipioFim = this.textoOuNulo(dados.municipioFim);
    }
    if (dados.ufFim !== undefined) atualizado.ufFim = this.normalizarUf(dados.ufFim);
    if (dados.remetenteNome !== undefined) {
      atualizado.remetenteNome = this.textoOuNulo(dados.remetenteNome);
    }
    if (dados.remetenteCnpj !== undefined) {
      atualizado.remetenteCnpj = this.textoOuNulo(dados.remetenteCnpj);
    }
    if (dados.destinatarioNome !== undefined) {
      atualizado.destinatarioNome = this.textoOuNulo(dados.destinatarioNome);
    }
    if (dados.destinatarioCnpj !== undefined) {
      atualizado.destinatarioCnpj = this.textoOuNulo(dados.destinatarioCnpj);
    }
    if (dados.tomadorNome !== undefined) {
      atualizado.tomadorNome = this.textoOuNulo(dados.tomadorNome);
    }
    if (dados.tomadorCnpj !== undefined) {
      atualizado.tomadorCnpj = this.textoOuNulo(dados.tomadorCnpj);
    }
    if (dados.motoristaNome !== undefined) {
      atualizado.motoristaNome = this.textoOuNulo(dados.motoristaNome);
    }
    if (dados.motoristaCpf !== undefined) {
      atualizado.motoristaCpf = this.textoOuNulo(dados.motoristaCpf);
    }
    if (dados.placaTracao !== undefined) {
      atualizado.placaTracao = this.normalizarPlaca(dados.placaTracao);
    }
    if (dados.placaReboque1 !== undefined) {
      atualizado.placaReboque1 = this.normalizarPlaca(dados.placaReboque1);
    }
    if (dados.placaReboque2 !== undefined) {
      atualizado.placaReboque2 = this.normalizarPlaca(dados.placaReboque2);
    }
    if (dados.placaReboque3 !== undefined) {
      atualizado.placaReboque3 = this.normalizarPlaca(dados.placaReboque3);
    }
    if (dados.valorTotalPrestacao !== undefined) {
      atualizado.valorTotalPrestacao = dados.valorTotalPrestacao;
    }
    if (dados.valorReceber !== undefined) atualizado.valorReceber = dados.valorReceber;
    if (dados.valorIcms !== undefined) atualizado.valorIcms = dados.valorIcms;
    if (dados.valorCarga !== undefined) atualizado.valorCarga = dados.valorCarga;
    if (dados.pesoBruto !== undefined) atualizado.pesoBruto = dados.pesoBruto;
    if (dados.quantidadeVolumes !== undefined) {
      atualizado.quantidadeVolumes = dados.quantidadeVolumes;
    }
    if (dados.chaveMdfe !== undefined) {
      atualizado.chaveMdfe = this.textoOuNulo(dados.chaveMdfe);
    }
    if (dados.qrCodeUrl !== undefined) {
      atualizado.qrCodeUrl = this.textoOuNulo(dados.qrCodeUrl);
    }
    if (dados.observacao !== undefined) {
      atualizado.observacao = this.textoOuNulo(dados.observacao);
    }
    if (dados.ativo !== undefined) atualizado.ativo = Boolean(dados.ativo);

    return atualizado;
  }

  private mapearCte(item: CteEntity): CteNormalizado {
    return {
      idCte: this.toInt(item.idCte),
      idEmpresa: this.toInt(item.idEmpresa),
      numeroCte: item.numeroCte,
      serie: item.serie,
      chaveCte: item.chaveCte,
      statusDocumento: item.statusDocumento,
      cstat: item.cstat ?? null,
      motivoStatus: item.motivoStatus,
      protocolo: item.protocolo,
      dataEmissao: item.dataEmissao,
      dataAutorizacao: item.dataAutorizacao,
      cfop: item.cfop,
      naturezaOperacao: item.naturezaOperacao,
      municipioInicio: item.municipioInicio,
      ufInicio: item.ufInicio,
      municipioFim: item.municipioFim,
      ufFim: item.ufFim,
      remetenteNome: item.remetenteNome,
      remetenteCnpj: item.remetenteCnpj,
      destinatarioNome: item.destinatarioNome,
      destinatarioCnpj: item.destinatarioCnpj,
      tomadorNome: item.tomadorNome,
      tomadorCnpj: item.tomadorCnpj,
      motoristaNome: item.motoristaNome,
      motoristaCpf: item.motoristaCpf,
      placaTracao: item.placaTracao,
      placaReboque1: item.placaReboque1,
      placaReboque2: item.placaReboque2,
      placaReboque3: item.placaReboque3,
      valorTotalPrestacao: this.toNumber(item.valorTotalPrestacao),
      valorReceber: this.toNumber(item.valorReceber),
      valorIcms: this.toNumber(item.valorIcms),
      valorCarga: this.toNumber(item.valorCarga),
      pesoBruto: this.toNumber(item.pesoBruto),
      quantidadeVolumes: this.toNumber(item.quantidadeVolumes),
      chaveMdfe: item.chaveMdfe,
      qrCodeUrl: item.qrCodeUrl,
      observacao: item.observacao,
      ativo: item.ativo,
      usuarioAtualizacao: item.usuarioAtualizacao,
      criadoEm: item.criadoEm,
      atualizadoEm: item.atualizadoEm,
    };
  }

  private validarIntervaloData(inicio?: string, fim?: string) {
    if (!inicio || !fim) return;
    const dataInicio = new Date(inicio);
    const dataFim = new Date(fim);
    this.validarData(dataInicio, 'dataEmissaoDe');
    this.validarData(dataFim, 'dataEmissaoAte');
    if (dataFim < dataInicio) {
      throw new BadRequestException(
        'Filtro invalido: dataEmissaoAte deve ser maior ou igual a dataEmissaoDe.',
      );
    }
  }

  private validarData(data: Date, campo: string) {
    if (Number.isNaN(data.getTime())) {
      throw new BadRequestException(`${campo} invalida.`);
    }
  }

  private textoOuNulo(valor: string | null | undefined) {
    if (valor === undefined || valor === null) return null;
    const texto = String(valor).trim();
    return texto.length > 0 ? texto : null;
  }

  private normalizarUf(valor?: string | null) {
    const texto = this.textoOuNulo(valor);
    return texto ? texto.toUpperCase() : null;
  }

  private normalizarPlaca(valor?: string | null) {
    const texto = this.textoOuNulo(valor);
    return texto ? texto.toUpperCase().replace(/\s+/g, '') : null;
  }

  private normalizarUsuario(usuarioJwt: JwtUsuarioPayload) {
    const fonte = usuarioJwt.nomeUsuario?.trim() || usuarioJwt.email.trim();
    return fonte.toUpperCase();
  }

  private toInt(valor: string | number | null | undefined) {
    if (valor === null || valor === undefined) return 0;
    const numero = Number(valor);
    return Number.isFinite(numero) ? Math.trunc(numero) : 0;
  }

  private toNumber(valor: string | number | null | undefined) {
    if (valor === null || valor === undefined) return 0;
    const numero = Number(valor);
    return Number.isFinite(numero) ? numero : 0;
  }

  private tratarErroPersistencia(
    error: unknown,
    acao: 'cadastrar' | 'atualizar',
  ): never {
    if (
      error instanceof BadRequestException ||
      error instanceof NotFoundException
    ) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as {
        code?: string;
        message?: string;
      };

      this.logger.error(
        `Falha ao ${acao} CT-e. code=${erroPg.code ?? 'N/A'} message=${erroPg.message ?? 'Erro desconhecido'}`,
      );

      if (erroPg.code === '23505') {
        throw new BadRequestException(
          'Ja existe CT-e com a mesma chave ou numero/serie para esta empresa.',
        );
      }
      if (erroPg.code === '23514') {
        throw new BadRequestException('Dados do CT-e invalidos para a estrutura atual.');
      }
      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Usuario do banco sem permissao para gravar em app.ctes.',
        );
      }
      if (erroPg.code === '42P01') {
        throw new BadRequestException('Tabela app.ctes nao encontrada.');
      }
      if (erroPg.code === '42703') {
        throw new BadRequestException(
          'Estrutura da tabela app.ctes esta diferente do esperado.',
        );
      }
    }

    this.logger.error(
      `Falha ao ${acao} CT-e sem codigo SQL mapeado. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
    );
    throw new BadRequestException(
      `Nao foi possivel ${acao} o CT-e neste momento.`,
    );
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (
      cteRepository: Repository<CteEntity>,
      manager: EntityManager,
    ) => Promise<T>,
  ): Promise<T> {
    return this.cteRepository.manager.transaction(async (manager) => {
      await configurarContextoEmpresaRls(manager, idEmpresa);
      const cteRepository = manager.getRepository(CteEntity);
      return callback(cteRepository, manager);
    });
  }
}
