import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import {
  EntityManager,
  IsNull,
  Not,
  QueryFailedError,
  Repository,
} from 'typeorm';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { AtualizarViagemDto } from './dto/atualizar-viagem.dto';
import { CriarViagemDto } from './dto/criar-viagem.dto';
import { FiltroViagensDto } from './dto/filtro-viagens.dto';
import { ViagemEntity } from './entities/viagem.entity';

type ViagemNormalizada = {
  idViagem: number;
  idEmpresa: number;
  idVeiculo: number;
  idMotorista: number;
  dataInicio: Date;
  dataFim: Date | null;
  kmInicial: number;
  kmFinal: number | null;
  status: string;
  observacao: string | null;
  valorFrete: number | null;
  media: number | null;
  totalDespesas: number | null;
  totalAbastecimentos: number | null;
  totalKm: number | null;
  totalLucro: number | null;
  peso: number | null;
  criadoEm: Date;
  atualizadoEm: Date;
  usuarioAtualizacao: string | null;
};

@Injectable()
export class ViagensService {
  private readonly logger = new Logger(ViagensService.name);

  private readonly colunasOrdenacao: Record<string, string> = {
    id_viagem: 'viagem.idViagem',
    data_inicio: 'viagem.dataInicio',
    data_fim: 'viagem.dataFim',
    criado_em: 'viagem.criadoEm',
    atualizado_em: 'viagem.atualizadoEm',
    km_inicial: 'viagem.kmInicial',
    km_final: 'viagem.kmFinal',
    valor_frete: 'viagem.valorFrete',
    total_despesas: 'viagem.totalDespesas',
    total_lucro: 'viagem.totalLucro',
  };

  constructor(
    @InjectRepository(ViagemEntity)
    private readonly viagemRepository: Repository<ViagemEntity>,
  ) {}

  async listarTodas(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (viagemRepository, manager) => {
      const viagens = await viagemRepository.find({
        where: { idEmpresa: String(idEmpresa), status: Not('I') },
        order: { dataInicio: 'DESC', idViagem: 'DESC' },
      });
      const idsViagem = viagens.map((viagem) => viagem.idViagem);
      const [pesoPorViagem, totalAbastecimentosPorViagem] = await Promise.all([
        this.carregarMapaPesoViagens(manager, idEmpresa, idsViagem),
        this.carregarMapaTotalAbastecimentosVinculados(
          manager,
          idsViagem,
        ),
      ]);

      return {
        sucesso: true,
        total: viagens.length,
        viagens: viagens.map((viagem) =>
          this.mapearViagem(
            viagem,
            pesoPorViagem.get(viagem.idViagem) ?? null,
            totalAbastecimentosPorViagem?.get(viagem.idViagem) ?? null,
          ),
        ),
      };
    });
  }

  async listarVinculaveisDespesa(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (viagemRepository, manager) => {
      const viagens = await viagemRepository.find({
        where: {
          idEmpresa: String(idEmpresa),
          status: 'A',
          dataFim: IsNull(),
        },
        order: { dataInicio: 'DESC', idViagem: 'DESC' },
      });
      const idsViagem = viagens.map((viagem) => viagem.idViagem);
      const [pesoPorViagem, totalAbastecimentosPorViagem] = await Promise.all([
        this.carregarMapaPesoViagens(manager, idEmpresa, idsViagem),
        this.carregarMapaTotalAbastecimentosVinculados(
          manager,
          idsViagem,
        ),
      ]);

      return {
        sucesso: true,
        total: viagens.length,
        viagens: viagens.map((viagem) =>
          this.mapearViagem(
            viagem,
            pesoPorViagem.get(viagem.idViagem) ?? null,
            totalAbastecimentosPorViagem?.get(viagem.idViagem) ?? null,
          ),
        ),
      };
    });
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroViagensDto) {
    this.validarIntervalosDoFiltro(filtro);
    return this.executarComRls(idEmpresa, async (viagemRepository, manager) => {
      const pagina = filtro.pagina ?? 1;
      const limite = filtro.limite ?? 30;
      const offset = (pagina - 1) * limite;

      const query = viagemRepository
        .createQueryBuilder('viagem')
        .where('viagem.idEmpresa = :idEmpresa', {
          idEmpresa: String(idEmpresa),
        });

      if (filtro.idViagem !== undefined) {
        query.andWhere('viagem.idViagem = :idViagem', {
          idViagem: filtro.idViagem,
        });
      }

      if (filtro.idVeiculo !== undefined) {
        query.andWhere('viagem.idVeiculo = :idVeiculo', {
          idVeiculo: filtro.idVeiculo,
        });
      }

      if (filtro.idMotorista !== undefined) {
        query.andWhere('viagem.idMotorista = :idMotorista', {
          idMotorista: filtro.idMotorista,
        });
      }

      if (filtro.status !== undefined) {
        query.andWhere('viagem.status = :status', {
          status: filtro.status,
        });
      } else {
        query.andWhere("COALESCE(viagem.status, 'A') <> :statusInativo", {
          statusInativo: 'I',
        });
      }

      if (filtro.texto !== undefined) {
        query.andWhere("COALESCE(viagem.observacao, '') ILIKE :texto", {
          texto: `%${filtro.texto}%`,
        });
      }

      if (filtro.dataInicioDe !== undefined) {
        query.andWhere('viagem.dataInicio >= :dataInicioDe', {
          dataInicioDe: filtro.dataInicioDe,
        });
      }

      if (filtro.dataInicioAte !== undefined) {
        query.andWhere('viagem.dataInicio <= :dataInicioAte', {
          dataInicioAte: filtro.dataInicioAte,
        });
      }

      if (filtro.dataFimDe !== undefined) {
        query.andWhere('viagem.dataFim >= :dataFimDe', {
          dataFimDe: filtro.dataFimDe,
        });
      }

      if (filtro.dataFimAte !== undefined) {
        query.andWhere('viagem.dataFim <= :dataFimAte', {
          dataFimAte: filtro.dataFimAte,
        });
      }

      if (filtro.kmInicialMin !== undefined) {
        query.andWhere('viagem.kmInicial >= :kmInicialMin', {
          kmInicialMin: filtro.kmInicialMin,
        });
      }

      if (filtro.kmInicialMax !== undefined) {
        query.andWhere('viagem.kmInicial <= :kmInicialMax', {
          kmInicialMax: filtro.kmInicialMax,
        });
      }

      if (filtro.kmFinalMin !== undefined) {
        query.andWhere('viagem.kmFinal >= :kmFinalMin', {
          kmFinalMin: filtro.kmFinalMin,
        });
      }

      if (filtro.kmFinalMax !== undefined) {
        query.andWhere('viagem.kmFinal <= :kmFinalMax', {
          kmFinalMax: filtro.kmFinalMax,
        });
      }

      if (filtro.valorFreteMin !== undefined) {
        query.andWhere('viagem.valorFrete >= :valorFreteMin', {
          valorFreteMin: filtro.valorFreteMin,
        });
      }

      if (filtro.valorFreteMax !== undefined) {
        query.andWhere('viagem.valorFrete <= :valorFreteMax', {
          valorFreteMax: filtro.valorFreteMax,
        });
      }

      if (filtro.totalDespesasMin !== undefined) {
        query.andWhere('viagem.totalDespesas >= :totalDespesasMin', {
          totalDespesasMin: filtro.totalDespesasMin,
        });
      }

      if (filtro.totalDespesasMax !== undefined) {
        query.andWhere('viagem.totalDespesas <= :totalDespesasMax', {
          totalDespesasMax: filtro.totalDespesasMax,
        });
      }

      if (filtro.totalLucroMin !== undefined) {
        query.andWhere('viagem.totalLucro >= :totalLucroMin', {
          totalLucroMin: filtro.totalLucroMin,
        });
      }

      if (filtro.totalLucroMax !== undefined) {
        query.andWhere('viagem.totalLucro <= :totalLucroMax', {
          totalLucroMax: filtro.totalLucroMax,
        });
      }

      if (filtro.apenasAbertas === true) {
        query.andWhere('viagem.dataFim IS NULL');
      } else if (filtro.apenasAbertas === false) {
        query.andWhere('viagem.dataFim IS NOT NULL');
      }

      const ordem = filtro.ordem ?? 'DESC';
      const colunaOrdenacao =
        this.colunasOrdenacao[filtro.ordenarPor ?? 'data_inicio'] ??
        this.colunasOrdenacao.data_inicio;

      query
        .orderBy(colunaOrdenacao, ordem)
        .addOrderBy('viagem.idViagem', 'DESC')
        .skip(offset)
        .take(limite);

      const [viagens, total] = await query.getManyAndCount();
      const idsViagem = viagens.map((viagem) => viagem.idViagem);
      const [pesoPorViagem, totalAbastecimentosPorViagem] = await Promise.all([
        this.carregarMapaPesoViagens(manager, idEmpresa, idsViagem),
        this.carregarMapaTotalAbastecimentosVinculados(
          manager,
          idsViagem,
        ),
      ]);

      return {
        sucesso: true,
        pagina,
        limite,
        total,
        paginas: total > 0 ? Math.ceil(total / limite) : 0,
        viagens: viagens.map((viagem) =>
          this.mapearViagem(
            viagem,
            pesoPorViagem.get(viagem.idViagem) ?? null,
            totalAbastecimentosPorViagem?.get(viagem.idViagem) ?? null,
          ),
        ),
      };
    });
  }

  async buscarPorId(idEmpresa: number, idViagem: number) {
    return this.executarComRls(idEmpresa, async (viagemRepository, manager) => {
      const viagem = await this.buscarViagemPorIdOuFalhar(
        viagemRepository,
        idEmpresa,
        idViagem,
      );
      const [peso, totalAbastecimentosPorViagem] = await Promise.all([
        this.carregarPesoDaViagem(manager, idEmpresa, idViagem),
        this.carregarMapaTotalAbastecimentosVinculados(manager, [idViagem]),
      ]);

      return {
        sucesso: true,
        viagem: this.mapearViagem(
          viagem,
          peso ?? null,
          totalAbastecimentosPorViagem?.get(idViagem) ?? null,
        ),
      };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarViagemDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const payload = this.normalizarCriacao(dados);
    this.validarConsistenciaDaViagem({
      dataInicio: payload.dataInicio,
      dataFim: payload.dataFim,
      kmInicial: payload.kmInicial,
      kmFinal: payload.kmFinal,
      peso: payload.peso,
    });

    try {
      return this.executarComRls(
        idEmpresa,
        async (viagemRepository, manager) => {
          const viagem = await viagemRepository.save(
            viagemRepository.create({
              idEmpresa: String(idEmpresa),
              idVeiculo: payload.idVeiculo,
              idMotorista: payload.idMotorista,
              dataInicio: payload.dataInicio,
              dataFim: payload.dataFim,
              kmInicial: payload.kmInicial,
              kmFinal: payload.kmFinal,
              status: payload.status,
              observacao: payload.observacao,
              valorFrete: payload.valorFrete,
              media: payload.media,
              totalDespesas: payload.totalDespesas,
              totalAbastecimentos: payload.totalAbastecimentos,
              totalKm: payload.totalKm,
              totalLucro: payload.totalLucro,
              usuarioAtualizacao:
                payload.usuarioAtualizacao ??
                this.normalizarUsuarioAtualizacao(usuarioJwt.email),
            }),
          );
          if (payload.peso !== undefined) {
            await this.atualizarPesoViagem(
              manager,
              idEmpresa,
              viagem.idViagem,
              payload.peso,
              payload.usuarioAtualizacao ??
                this.normalizarUsuarioAtualizacao(usuarioJwt.email),
            );
          }
          const [peso, totalAbastecimentosPorViagem] = await Promise.all([
            this.carregarPesoDaViagem(manager, idEmpresa, viagem.idViagem),
            this.carregarMapaTotalAbastecimentosVinculados(manager, [
              viagem.idViagem,
            ]),
          ]);

          return {
            sucesso: true,
            mensagem: 'Viagem cadastrada com sucesso.',
            viagem: this.mapearViagem(
              viagem,
              peso ?? null,
              totalAbastecimentosPorViagem?.get(viagem.idViagem) ?? null,
            ),
          };
        },
      );
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar');
    }
  }

  async atualizar(
    idEmpresa: number,
    idViagem: number,
    dados: AtualizarViagemDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(
        idEmpresa,
        async (viagemRepository, manager) => {
          const viagemAtual = await this.buscarViagemPorIdOuFalhar(
            viagemRepository,
            idEmpresa,
            idViagem,
          );
          const payload = this.normalizarAtualizacao(dados);

          const dataInicio = payload.dataInicio ?? viagemAtual.dataInicio;
          const dataFim =
            payload.dataFim !== undefined
              ? payload.dataFim
              : viagemAtual.dataFim;
          const kmInicial = payload.kmInicial ?? viagemAtual.kmInicial;
          const kmFinal =
            payload.kmFinal !== undefined
              ? payload.kmFinal
              : viagemAtual.kmFinal;

          this.validarConsistenciaDaViagem({
            dataInicio,
            dataFim,
            kmInicial,
            kmFinal,
            peso: payload.peso,
          });

          const { peso: _pesoIgnorado, ...payloadSemPeso } = payload;
          await viagemRepository.update(
            { idViagem, idEmpresa: String(idEmpresa) },
            {
              ...payloadSemPeso,
              usuarioAtualizacao:
                payload.usuarioAtualizacao ??
                this.normalizarUsuarioAtualizacao(usuarioJwt.email),
            },
          );
          if (payload.peso !== undefined) {
            await this.atualizarPesoViagem(
              manager,
              idEmpresa,
              idViagem,
              payload.peso,
              payload.usuarioAtualizacao ??
                this.normalizarUsuarioAtualizacao(usuarioJwt.email),
            );
          }

          const viagemAtualizada = await this.buscarViagemPorIdOuFalhar(
            viagemRepository,
            idEmpresa,
            idViagem,
          );
          const [pesoAtualizado, totalAbastecimentosPorViagem] =
            await Promise.all([
              this.carregarPesoDaViagem(manager, idEmpresa, idViagem),
              this.carregarMapaTotalAbastecimentosVinculados(manager, [
                idViagem,
              ]),
            ]);

          return {
            sucesso: true,
            mensagem: 'Viagem atualizada com sucesso.',
            viagem: this.mapearViagem(
              viagemAtualizada,
              pesoAtualizado ?? null,
              totalAbastecimentosPorViagem?.get(idViagem) ?? null,
            ),
          };
        },
      );
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  async remover(idEmpresa: number, idViagem: number) {
    return this.executarComRls(idEmpresa, async (viagemRepository) => {
      const viagem = await this.buscarViagemPorIdOuFalhar(
        viagemRepository,
        idEmpresa,
        idViagem,
      );

      if ((viagem.status ?? '').trim().toUpperCase() === 'I') {
        throw new BadRequestException('Viagem ja esta inativa.');
      }

      await viagemRepository.update(
        { idViagem, idEmpresa: String(idEmpresa) },
        {
          status: 'I',
          atualizadoEm: new Date(),
        },
      );

      return {
        sucesso: true,
        mensagem: 'Viagem inativada com sucesso.',
        idViagem,
      };
    });
  }

  private async buscarViagemPorIdOuFalhar(
    viagemRepository: Repository<ViagemEntity>,
    idEmpresa: number,
    idViagem: number,
  ) {
    const viagem = await viagemRepository.findOne({
      where: {
        idViagem,
        idEmpresa: String(idEmpresa),
      },
    });

    if (!viagem) {
      throw new NotFoundException(
        'Viagem nao encontrada para a empresa logada.',
      );
    }

    return viagem;
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (
      viagemRepository: Repository<ViagemEntity>,
      manager: EntityManager,
    ) => Promise<T>,
  ): Promise<T> {
    return this.viagemRepository.manager.transaction(async (manager) => {
      await configurarContextoEmpresaRls(manager, idEmpresa);
      const viagemRepository = manager.getRepository(ViagemEntity);
      return callback(viagemRepository, manager);
    });
  }

  private normalizarCriacao(dados: CriarViagemDto) {
    return {
      idVeiculo: dados.idVeiculo,
      idMotorista: dados.idMotorista,
      dataInicio: new Date(dados.dataInicio),
      dataFim: dados.dataFim ? new Date(dados.dataFim) : null,
      kmInicial: dados.kmInicial,
      kmFinal: dados.kmFinal ?? null,
      status: (dados.status ?? 'A').trim().toUpperCase(),
      observacao: dados.observacao?.trim()
        ? dados.observacao.trim().toUpperCase()
        : null,
      valorFrete: dados.valorFrete ?? 0,
      media: dados.media ?? 0,
      totalDespesas: dados.totalDespesas ?? 0,
      totalAbastecimentos: dados.totalAbastecimentos ?? 0,
      totalKm: dados.totalKm ?? 0,
      totalLucro: dados.totalLucro ?? 0,
      peso: dados.peso,
      usuarioAtualizacao: dados.usuarioAtualizacao?.trim()
        ? this.normalizarUsuarioAtualizacao(dados.usuarioAtualizacao)
        : null,
    };
  }

  private normalizarAtualizacao(dados: AtualizarViagemDto) {
    return {
      idVeiculo: dados.idVeiculo,
      idMotorista: dados.idMotorista,
      dataInicio:
        dados.dataInicio !== undefined ? new Date(dados.dataInicio) : undefined,
      dataFim:
        dados.dataFim !== undefined ? new Date(dados.dataFim) : undefined,
      kmInicial: dados.kmInicial,
      kmFinal: dados.kmFinal,
      status:
        dados.status !== undefined
          ? dados.status.trim().toUpperCase()
          : undefined,
      observacao:
        dados.observacao !== undefined
          ? dados.observacao.trim() || null
          : undefined,
      valorFrete: dados.valorFrete,
      media: dados.media,
      totalDespesas: dados.totalDespesas,
      totalAbastecimentos: dados.totalAbastecimentos,
      totalKm: dados.totalKm,
      totalLucro: dados.totalLucro,
      peso: dados.peso,
      usuarioAtualizacao:
        dados.usuarioAtualizacao !== undefined
          ? this.normalizarUsuarioAtualizacao(dados.usuarioAtualizacao)
          : undefined,
    };
  }

  private validarIntervalosDoFiltro(filtro: FiltroViagensDto) {
    if (
      filtro.dataInicioDe &&
      filtro.dataInicioAte &&
      new Date(filtro.dataInicioAte) < new Date(filtro.dataInicioDe)
    ) {
      throw new BadRequestException(
        'Filtro invalido: dataInicioAte deve ser maior ou igual a dataInicioDe.',
      );
    }

    if (
      filtro.dataFimDe &&
      filtro.dataFimAte &&
      new Date(filtro.dataFimAte) < new Date(filtro.dataFimDe)
    ) {
      throw new BadRequestException(
        'Filtro invalido: dataFimAte deve ser maior ou igual a dataFimDe.',
      );
    }

    if (
      filtro.kmInicialMin !== undefined &&
      filtro.kmInicialMax !== undefined &&
      filtro.kmInicialMax < filtro.kmInicialMin
    ) {
      throw new BadRequestException(
        'Filtro invalido: kmInicialMax deve ser maior ou igual a kmInicialMin.',
      );
    }

    if (
      filtro.kmFinalMin !== undefined &&
      filtro.kmFinalMax !== undefined &&
      filtro.kmFinalMax < filtro.kmFinalMin
    ) {
      throw new BadRequestException(
        'Filtro invalido: kmFinalMax deve ser maior ou igual a kmFinalMin.',
      );
    }

    if (
      filtro.valorFreteMin !== undefined &&
      filtro.valorFreteMax !== undefined &&
      filtro.valorFreteMax < filtro.valorFreteMin
    ) {
      throw new BadRequestException(
        'Filtro invalido: valorFreteMax deve ser maior ou igual a valorFreteMin.',
      );
    }

    if (
      filtro.totalDespesasMin !== undefined &&
      filtro.totalDespesasMax !== undefined &&
      filtro.totalDespesasMax < filtro.totalDespesasMin
    ) {
      throw new BadRequestException(
        'Filtro invalido: totalDespesasMax deve ser maior ou igual a totalDespesasMin.',
      );
    }

    if (
      filtro.totalLucroMin !== undefined &&
      filtro.totalLucroMax !== undefined &&
      filtro.totalLucroMax < filtro.totalLucroMin
    ) {
      throw new BadRequestException(
        'Filtro invalido: totalLucroMax deve ser maior ou igual a totalLucroMin.',
      );
    }
  }

  private validarConsistenciaDaViagem(params: {
    dataInicio: Date;
    dataFim: Date | null;
    kmInicial: number;
    kmFinal: number | null;
    peso?: number;
  }) {
    if (Number.isNaN(params.dataInicio.getTime())) {
      throw new BadRequestException('dataInicio invalida.');
    }

    if (params.dataFim && Number.isNaN(params.dataFim.getTime())) {
      throw new BadRequestException('dataFim invalida.');
    }

    if (params.dataFim && params.dataFim < params.dataInicio) {
      throw new BadRequestException(
        'dataFim nao pode ser menor que dataInicio.',
      );
    }

    if (params.kmFinal !== null && params.kmFinal < params.kmInicial) {
      throw new BadRequestException(
        'kmFinal nao pode ser menor que kmInicial.',
      );
    }

    if (
      params.peso !== undefined &&
      (!Number.isFinite(params.peso) || params.peso < 0)
    ) {
      throw new BadRequestException('peso invalido.');
    }
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
        `Falha ao ${acao} viagem. code=${erroPg.code ?? 'N/A'} message=${erroPg.message ?? 'Erro desconhecido'}`,
      );

      if (erroPg.code === '23503') {
        throw new BadRequestException(
          'idVeiculo, idMotorista ou idEmpresa informado nao existe.',
        );
      }

      if (erroPg.code === '23514') {
        throw new BadRequestException(
          'Dados invalidos para data ou quilometragem da viagem.',
        );
      }

      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Usuario do banco sem permissao para gravar em app.viagens.',
        );
      }

      if (erroPg.code === '42P01') {
        throw new BadRequestException('Tabela app.viagens nao encontrada.');
      }

      if (erroPg.code === '42703') {
        throw new BadRequestException(
          'Estrutura da tabela app.viagens esta diferente do esperado.',
        );
      }
    }

    this.logger.error(
      `Falha ao ${acao} viagem sem codigo SQL mapeado. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
    );
    throw new BadRequestException(
      `Nao foi possivel ${acao} a viagem neste momento.`,
    );
  }

  private normalizarUsuarioAtualizacao(valor: string): string {
    return valor.trim().toUpperCase();
  }

  private async colunaPesoExiste(manager: EntityManager): Promise<boolean> {
    const rows = await manager.query(
      `
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'app'
        AND table_name = 'viagens'
        AND column_name = 'peso'
      LIMIT 1
      `,
    );
    return rows.length > 0;
  }

  private async colunaIdViagemAbastecimentoExiste(
    manager: EntityManager,
  ): Promise<boolean> {
    const rows = await manager.query(
      `
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'app'
        AND table_name = 'abastecimentos'
        AND column_name = 'id_viagem'
      LIMIT 1
      `,
    );

    return rows.length > 0;
  }

  private async carregarMapaTotalAbastecimentosVinculados(
    manager: EntityManager,
    idsViagem: number[],
  ): Promise<Map<number, number> | null> {
    const idsValidos = Array.from(
      new Set(
        idsViagem
          .filter((idViagem) => Number.isFinite(idViagem) && idViagem > 0)
          .map((idViagem) => Math.trunc(idViagem)),
      ),
    );

    const mapa = new Map<number, number>();
    if (idsValidos.length === 0) {
      return mapa;
    }

    const colunaExiste = await this.colunaIdViagemAbastecimentoExiste(manager);
    if (!colunaExiste) {
      return null;
    }

    idsValidos.forEach((idViagem) => {
      mapa.set(idViagem, 0);
    });

    const rows = (await manager.query(
      `
      SELECT
        id_viagem,
        COALESCE(
          SUM(
            COALESCE(
              valor_total::numeric,
              COALESCE(litros::numeric, 0) * COALESCE(valor_litro::numeric, 0)
            )
          ),
          0
        )::numeric AS total_abastecimentos
      FROM app.abastecimentos
      WHERE id_viagem = ANY($1::int[])
      GROUP BY id_viagem
      `,
      [idsValidos],
    )) as Array<{ id_viagem?: unknown; total_abastecimentos?: unknown }>;

    rows.forEach((row) => {
      const idViagem = Number(row.id_viagem);
      if (!Number.isFinite(idViagem)) {
        return;
      }

      mapa.set(
        idViagem,
        this.converterNumero(
          row.total_abastecimentos as string | number | null,
        ) ?? 0,
      );
    });

    return mapa;
  }

  private async carregarMapaPesoViagens(
    manager: EntityManager,
    idEmpresa: number,
    idsViagem: number[],
  ): Promise<Map<number, number | null>> {
    const mapa = new Map<number, number | null>();
    if (idsViagem.length === 0) {
      return mapa;
    }

    const colunaExiste = await this.colunaPesoExiste(manager);
    if (!colunaExiste) {
      return mapa;
    }

    const rows = await manager.query(
      `
      SELECT id_viagem, peso
      FROM app.viagens
      WHERE id_empresa = $1
        AND id_viagem = ANY($2::int[])
      `,
      [String(idEmpresa), idsViagem],
    );

    rows.forEach((row) => {
      const idViagem = Number(row.id_viagem);
      if (!Number.isFinite(idViagem)) {
        return;
      }
      mapa.set(
        idViagem,
        this.converterNumero(row.peso as string | number | null),
      );
    });

    return mapa;
  }

  private async carregarPesoDaViagem(
    manager: EntityManager,
    idEmpresa: number,
    idViagem: number,
  ): Promise<number | null> {
    const colunaExiste = await this.colunaPesoExiste(manager);
    if (!colunaExiste) {
      return null;
    }

    const rows = await manager.query(
      `
      SELECT peso
      FROM app.viagens
      WHERE id_empresa = $1
        AND id_viagem = $2
      LIMIT 1
      `,
      [String(idEmpresa), idViagem],
    );

    return this.converterNumero(
      (rows[0]?.peso as string | number | null) ?? null,
    );
  }

  private async atualizarPesoViagem(
    manager: EntityManager,
    idEmpresa: number,
    idViagem: number,
    peso: number,
    usuarioAtualizacao: string,
  ): Promise<void> {
    const colunaExiste = await this.colunaPesoExiste(manager);
    if (!colunaExiste) {
      throw new BadRequestException(
        'Campo peso ainda nao esta disponivel na tabela app.viagens. Execute o script sql/viagens_add_peso.sql.',
      );
    }

    await manager.query(
      `
      UPDATE app.viagens
      SET
        peso = $1,
        atualizado_em = NOW(),
        usuario_atualizacao = $2
      WHERE id_empresa = $3
        AND id_viagem = $4
      `,
      [peso, usuarioAtualizacao, String(idEmpresa), idViagem],
    );
  }

  private mapearViagem(
    viagem: ViagemEntity,
    pesoExtra?: number | null,
    totalAbastecimentosVinculados?: number | null,
  ): ViagemNormalizada {
    const valorFrete = this.converterNumero(viagem.valorFrete);
    const totalDespesas = this.converterNumero(viagem.totalDespesas);
    const totalAbastecimentos =
      totalAbastecimentosVinculados ??
      this.converterNumero(viagem.totalAbastecimentos);
    const totalLucroPersistido = this.converterNumero(viagem.totalLucro);
    const totalLucroCalculado =
      valorFrete !== null
        ? valorFrete - (totalAbastecimentos ?? 0) - (totalDespesas ?? 0)
        : totalLucroPersistido;

    return {
      idViagem: viagem.idViagem,
      idEmpresa: Number(viagem.idEmpresa),
      idVeiculo: viagem.idVeiculo,
      idMotorista: viagem.idMotorista,
      dataInicio: viagem.dataInicio,
      dataFim: viagem.dataFim,
      kmInicial: viagem.kmInicial,
      kmFinal: viagem.kmFinal,
      status: viagem.status,
      observacao: viagem.observacao,
      valorFrete,
      media: this.converterNumero(viagem.media),
      totalDespesas,
      totalAbastecimentos,
      totalKm: viagem.totalKm,
      totalLucro: totalLucroCalculado,
      peso: pesoExtra ?? null,
      criadoEm: viagem.criadoEm,
      atualizadoEm: viagem.atualizadoEm,
      usuarioAtualizacao: viagem.usuarioAtualizacao,
    };
  }

  private converterNumero(valor: string | number | null): number | null {
    if (valor === null || valor === undefined) {
      return null;
    }

    const numero = Number(valor);
    return Number.isFinite(numero) ? numero : null;
  }
}
