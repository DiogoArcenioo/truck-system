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
import { AtualizarManifestoDto } from './dto/atualizar-manifesto.dto';
import { CriarManifestoDto } from './dto/criar-manifesto.dto';
import { FiltroManifestoDto } from './dto/filtro-manifesto.dto';
import { ManifestoEntity } from './entities/manifesto.entity';

type ManifestoNormalizado = {
  idManifesto: number;
  idEmpresa: number;
  numeroManifesto: number;
  serie: number;
  chaveMdfe: string;
  statusDocumento: string;
  cstat: number | null;
  motivoStatus: string | null;
  protocolo: string | null;
  dataEmissao: Date;
  dataAutorizacao: Date | null;
  dataInicioViagem: Date | null;
  ufInicio: string | null;
  ufFim: string | null;
  municipioCarregamento: string | null;
  percursoUfs: string | null;
  rntrc: string | null;
  placaTracao: string | null;
  placaReboque1: string | null;
  placaReboque2: string | null;
  placaReboque3: string | null;
  condutorNome: string | null;
  condutorCpf: string | null;
  quantidadeCte: number;
  chavesCte: string | null;
  valorCarga: number;
  quantidadeCarga: number;
  produtoPredominante: string | null;
  seguradoraNome: string | null;
  apoliceNumero: string | null;
  averbacaoNumero: string | null;
  qrCodeUrl: string | null;
  observacao: string | null;
  ativo: boolean;
  usuarioAtualizacao: string;
  criadoEm: Date;
  atualizadoEm: Date;
};

type OrdenacaoCampo =
  | 'id_manifesto'
  | 'numero_manifesto'
  | 'serie'
  | 'data_emissao'
  | 'data_inicio_viagem'
  | 'atualizado_em';

@Injectable()
export class ManifestoService {
  private readonly logger = new Logger(ManifestoService.name);

  private readonly colunasOrdenacao: Record<OrdenacaoCampo, string> = {
    id_manifesto: 'manifesto.idManifesto',
    numero_manifesto: 'manifesto.numeroManifesto',
    serie: 'manifesto.serie',
    data_emissao: 'manifesto.dataEmissao',
    data_inicio_viagem: 'manifesto.dataInicioViagem',
    atualizado_em: 'manifesto.atualizadoEm',
  };

  constructor(
    @InjectRepository(ManifestoEntity)
    private readonly manifestoRepository: Repository<ManifestoEntity>,
  ) {}

  async listarTodas(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (manifestoRepository) => {
      const itens = await manifestoRepository.find({
        where: { idEmpresa: String(idEmpresa), ativo: true },
        order: { dataEmissao: 'DESC', idManifesto: 'DESC' },
      });

      return {
        sucesso: true,
        total: itens.length,
        manifestos: itens.map((item) => this.mapearManifesto(item)),
      };
    });
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroManifestoDto) {
    this.validarIntervaloData(filtro.dataEmissaoDe, filtro.dataEmissaoAte);

    return this.executarComRls(idEmpresa, async (manifestoRepository) => {
      const pagina = filtro.pagina ?? 1;
      const limite = filtro.limite ?? 30;
      const offset = (pagina - 1) * limite;

      const query = manifestoRepository
        .createQueryBuilder('manifesto')
        .where('manifesto.idEmpresa = :idEmpresa', {
          idEmpresa: String(idEmpresa),
        });

      if (!filtro.incluirInativos) {
        query.andWhere('manifesto.ativo = true');
      }

      if (filtro.idManifesto !== undefined) {
        query.andWhere('manifesto.idManifesto = :idManifesto', {
          idManifesto: filtro.idManifesto,
        });
      }

      if (filtro.numeroManifesto !== undefined) {
        query.andWhere('manifesto.numeroManifesto = :numeroManifesto', {
          numeroManifesto: filtro.numeroManifesto,
        });
      }

      if (filtro.serie !== undefined) {
        query.andWhere('manifesto.serie = :serie', {
          serie: filtro.serie,
        });
      }

      if (filtro.chaveMdfe?.trim()) {
        query.andWhere('manifesto.chaveMdfe ILIKE :chaveMdfe', {
          chaveMdfe: `%${filtro.chaveMdfe.trim()}%`,
        });
      }

      if (filtro.statusDocumento) {
        query.andWhere('manifesto.statusDocumento = :statusDocumento', {
          statusDocumento: filtro.statusDocumento,
        });
      }

      if (filtro.texto?.trim()) {
        const texto = `%${filtro.texto.trim()}%`;
        query.andWhere(
          `
          (
            CAST(manifesto.numeroManifesto AS TEXT) ILIKE :texto
            OR manifesto.chaveMdfe ILIKE :texto
            OR COALESCE(manifesto.placaTracao, '') ILIKE :texto
            OR COALESCE(manifesto.condutorNome, '') ILIKE :texto
            OR COALESCE(manifesto.municipioCarregamento, '') ILIKE :texto
            OR COALESCE(manifesto.chavesCte, '') ILIKE :texto
          )
          `,
          { texto },
        );
      }

      if (filtro.dataEmissaoDe) {
        query.andWhere('manifesto.dataEmissao >= :dataEmissaoDe', {
          dataEmissaoDe: filtro.dataEmissaoDe,
        });
      }

      if (filtro.dataEmissaoAte) {
        query.andWhere('manifesto.dataEmissao <= :dataEmissaoAte', {
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
        manifestos: itens.map((item) => this.mapearManifesto(item)),
      };
    });
  }

  async buscarPorId(idEmpresa: number, idManifesto: number) {
    return this.executarComRls(idEmpresa, async (manifestoRepository) => {
      const item = await this.buscarPorIdOuFalhar(
        manifestoRepository,
        idEmpresa,
        idManifesto,
      );
      return { sucesso: true, manifesto: this.mapearManifesto(item) };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarManifestoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const payload = this.normalizarCriacao(dados, usuarioJwt);

    try {
      return this.executarComRls(idEmpresa, async (manifestoRepository) => {
        const salvo = await manifestoRepository.save(
          manifestoRepository.create({
            ...payload,
            idEmpresa: String(idEmpresa),
          }),
        );

        return {
          sucesso: true,
          mensagem: 'Manifesto cadastrado com sucesso.',
          manifesto: this.mapearManifesto(salvo),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar');
    }
  }

  async atualizar(
    idEmpresa: number,
    idManifesto: number,
    dados: AtualizarManifestoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const payload = this.normalizarAtualizacao(dados, usuarioJwt);

    try {
      return this.executarComRls(idEmpresa, async (manifestoRepository) => {
        await this.buscarPorIdOuFalhar(
          manifestoRepository,
          idEmpresa,
          idManifesto,
        );

        await manifestoRepository.update(
          { idManifesto: String(idManifesto), idEmpresa: String(idEmpresa) },
          payload,
        );

        const atualizado = await this.buscarPorIdOuFalhar(
          manifestoRepository,
          idEmpresa,
          idManifesto,
        );

        return {
          sucesso: true,
          mensagem: 'Manifesto atualizado com sucesso.',
          manifesto: this.mapearManifesto(atualizado),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  async remover(
    idEmpresa: number,
    idManifesto: number,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    return this.executarComRls(idEmpresa, async (manifestoRepository) => {
      const item = await this.buscarPorIdOuFalhar(
        manifestoRepository,
        idEmpresa,
        idManifesto,
      );

      if (!item.ativo) {
        throw new BadRequestException('Manifesto ja esta inativo.');
      }

      await manifestoRepository.update(
        { idManifesto: String(idManifesto), idEmpresa: String(idEmpresa) },
        {
          ativo: false,
          usuarioAtualizacao: this.normalizarUsuario(usuarioJwt),
          atualizadoEm: new Date(),
        },
      );

      return {
        sucesso: true,
        mensagem: 'Manifesto inativado com sucesso.',
        idManifesto,
      };
    });
  }

  private async buscarPorIdOuFalhar(
    manifestoRepository: Repository<ManifestoEntity>,
    idEmpresa: number,
    idManifesto: number,
  ) {
    const item = await manifestoRepository.findOne({
      where: { idManifesto: String(idManifesto), idEmpresa: String(idEmpresa) },
    });

    if (!item) {
      throw new NotFoundException(
        'Manifesto nao encontrado para a empresa logada.',
      );
    }

    return item;
  }

  private aplicarOrdenacao(
    query: SelectQueryBuilder<ManifestoEntity>,
    filtro: FiltroManifestoDto,
  ) {
    const campo = (filtro.ordenarPor ?? 'data_emissao') as OrdenacaoCampo;
    const ordem = filtro.ordem ?? 'DESC';
    const coluna =
      this.colunasOrdenacao[campo] ?? this.colunasOrdenacao.data_emissao;
    query.orderBy(coluna, ordem).addOrderBy('manifesto.idManifesto', 'DESC');
  }

  private normalizarCriacao(
    dados: CriarManifestoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const dataEmissao = new Date(dados.dataEmissao);
    const dataAutorizacao = dados.dataAutorizacao
      ? new Date(dados.dataAutorizacao)
      : null;
    const dataInicioViagem = dados.dataInicioViagem
      ? new Date(dados.dataInicioViagem)
      : null;

    this.validarData(dataEmissao, 'dataEmissao');
    if (dataAutorizacao) this.validarData(dataAutorizacao, 'dataAutorizacao');
    if (dataInicioViagem) this.validarData(dataInicioViagem, 'dataInicioViagem');

    return {
      numeroManifesto: dados.numeroManifesto,
      serie: dados.serie,
      chaveMdfe: dados.chaveMdfe.trim(),
      statusDocumento: (dados.statusDocumento ?? 'AUTORIZADO').toUpperCase(),
      cstat: dados.cstat ?? null,
      motivoStatus: this.textoOuNulo(dados.motivoStatus),
      protocolo: this.textoOuNulo(dados.protocolo),
      dataEmissao,
      dataAutorizacao,
      dataInicioViagem,
      ufInicio: this.normalizarUf(dados.ufInicio),
      ufFim: this.normalizarUf(dados.ufFim),
      municipioCarregamento: this.textoOuNulo(dados.municipioCarregamento),
      percursoUfs: this.textoOuNulo(dados.percursoUfs),
      rntrc: this.textoOuNulo(dados.rntrc),
      placaTracao: this.normalizarPlaca(dados.placaTracao),
      placaReboque1: this.normalizarPlaca(dados.placaReboque1),
      placaReboque2: this.normalizarPlaca(dados.placaReboque2),
      placaReboque3: this.normalizarPlaca(dados.placaReboque3),
      condutorNome: this.textoOuNulo(dados.condutorNome),
      condutorCpf: this.textoOuNulo(dados.condutorCpf),
      quantidadeCte: dados.quantidadeCte ?? 0,
      chavesCte: this.textoOuNulo(dados.chavesCte),
      valorCarga: dados.valorCarga ?? 0,
      quantidadeCarga: dados.quantidadeCarga ?? 0,
      produtoPredominante: this.textoOuNulo(dados.produtoPredominante),
      seguradoraNome: this.textoOuNulo(dados.seguradoraNome),
      apoliceNumero: this.textoOuNulo(dados.apoliceNumero),
      averbacaoNumero: this.textoOuNulo(dados.averbacaoNumero),
      qrCodeUrl: this.textoOuNulo(dados.qrCodeUrl),
      observacao: this.textoOuNulo(dados.observacao),
      ativo: true,
      usuarioAtualizacao:
        this.textoOuNulo(dados.usuarioAtualizacao) ??
        this.normalizarUsuario(usuarioJwt),
    };
  }

  private normalizarAtualizacao(
    dados: AtualizarManifestoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const atualizado: Partial<ManifestoEntity> = {
      usuarioAtualizacao:
        this.textoOuNulo(dados.usuarioAtualizacao) ??
        this.normalizarUsuario(usuarioJwt),
    };

    if (dados.numeroManifesto !== undefined) {
      atualizado.numeroManifesto = dados.numeroManifesto;
    }
    if (dados.serie !== undefined) atualizado.serie = dados.serie;
    if (dados.chaveMdfe !== undefined) atualizado.chaveMdfe = dados.chaveMdfe.trim();
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
    if (dados.dataInicioViagem !== undefined) {
      if (dados.dataInicioViagem === null) {
        atualizado.dataInicioViagem = null;
      } else {
        const data = new Date(dados.dataInicioViagem);
        this.validarData(data, 'dataInicioViagem');
        atualizado.dataInicioViagem = data;
      }
    }
    if (dados.ufInicio !== undefined) atualizado.ufInicio = this.normalizarUf(dados.ufInicio);
    if (dados.ufFim !== undefined) atualizado.ufFim = this.normalizarUf(dados.ufFim);
    if (dados.municipioCarregamento !== undefined) {
      atualizado.municipioCarregamento = this.textoOuNulo(
        dados.municipioCarregamento,
      );
    }
    if (dados.percursoUfs !== undefined) {
      atualizado.percursoUfs = this.textoOuNulo(dados.percursoUfs);
    }
    if (dados.rntrc !== undefined) atualizado.rntrc = this.textoOuNulo(dados.rntrc);
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
    if (dados.condutorNome !== undefined) {
      atualizado.condutorNome = this.textoOuNulo(dados.condutorNome);
    }
    if (dados.condutorCpf !== undefined) {
      atualizado.condutorCpf = this.textoOuNulo(dados.condutorCpf);
    }
    if (dados.quantidadeCte !== undefined) {
      atualizado.quantidadeCte = dados.quantidadeCte;
    }
    if (dados.chavesCte !== undefined) {
      atualizado.chavesCte = this.textoOuNulo(dados.chavesCte);
    }
    if (dados.valorCarga !== undefined) atualizado.valorCarga = dados.valorCarga;
    if (dados.quantidadeCarga !== undefined) {
      atualizado.quantidadeCarga = dados.quantidadeCarga;
    }
    if (dados.produtoPredominante !== undefined) {
      atualizado.produtoPredominante = this.textoOuNulo(
        dados.produtoPredominante,
      );
    }
    if (dados.seguradoraNome !== undefined) {
      atualizado.seguradoraNome = this.textoOuNulo(dados.seguradoraNome);
    }
    if (dados.apoliceNumero !== undefined) {
      atualizado.apoliceNumero = this.textoOuNulo(dados.apoliceNumero);
    }
    if (dados.averbacaoNumero !== undefined) {
      atualizado.averbacaoNumero = this.textoOuNulo(dados.averbacaoNumero);
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

  private mapearManifesto(item: ManifestoEntity): ManifestoNormalizado {
    return {
      idManifesto: this.toInt(item.idManifesto),
      idEmpresa: this.toInt(item.idEmpresa),
      numeroManifesto: item.numeroManifesto,
      serie: item.serie,
      chaveMdfe: item.chaveMdfe,
      statusDocumento: item.statusDocumento,
      cstat: item.cstat ?? null,
      motivoStatus: item.motivoStatus,
      protocolo: item.protocolo,
      dataEmissao: item.dataEmissao,
      dataAutorizacao: item.dataAutorizacao,
      dataInicioViagem: item.dataInicioViagem,
      ufInicio: item.ufInicio,
      ufFim: item.ufFim,
      municipioCarregamento: item.municipioCarregamento,
      percursoUfs: item.percursoUfs,
      rntrc: item.rntrc,
      placaTracao: item.placaTracao,
      placaReboque1: item.placaReboque1,
      placaReboque2: item.placaReboque2,
      placaReboque3: item.placaReboque3,
      condutorNome: item.condutorNome,
      condutorCpf: item.condutorCpf,
      quantidadeCte: item.quantidadeCte,
      chavesCte: item.chavesCte,
      valorCarga: this.toNumber(item.valorCarga),
      quantidadeCarga: this.toNumber(item.quantidadeCarga),
      produtoPredominante: item.produtoPredominante,
      seguradoraNome: item.seguradoraNome,
      apoliceNumero: item.apoliceNumero,
      averbacaoNumero: item.averbacaoNumero,
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
        `Falha ao ${acao} manifesto. code=${erroPg.code ?? 'N/A'} message=${erroPg.message ?? 'Erro desconhecido'}`,
      );

      if (erroPg.code === '23505') {
        throw new BadRequestException(
          'Ja existe manifesto com a mesma chave ou numero/serie para esta empresa.',
        );
      }
      if (erroPg.code === '23514') {
        throw new BadRequestException(
          'Dados do manifesto invalidos para a estrutura atual.',
        );
      }
      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Usuario do banco sem permissao para gravar em app.manifestos.',
        );
      }
      if (erroPg.code === '42P01') {
        throw new BadRequestException('Tabela app.manifestos nao encontrada.');
      }
      if (erroPg.code === '42703') {
        throw new BadRequestException(
          'Estrutura da tabela app.manifestos esta diferente do esperado.',
        );
      }
    }

    this.logger.error(
      `Falha ao ${acao} manifesto sem codigo SQL mapeado. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
    );
    throw new BadRequestException(
      `Nao foi possivel ${acao} o manifesto neste momento.`,
    );
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (
      manifestoRepository: Repository<ManifestoEntity>,
      manager: EntityManager,
    ) => Promise<T>,
  ): Promise<T> {
    return this.manifestoRepository.manager.transaction(async (manager) => {
      await configurarContextoEmpresaRls(manager, idEmpresa);
      const manifestoRepository = manager.getRepository(ManifestoEntity);
      return callback(manifestoRepository, manager);
    });
  }
}
