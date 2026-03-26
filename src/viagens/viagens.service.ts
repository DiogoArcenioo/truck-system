import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { QueryFailedError, Repository } from 'typeorm';
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
    const viagens = await this.viagemRepository.find({
      where: { idEmpresa: String(idEmpresa) },
      order: { dataInicio: 'DESC', idViagem: 'DESC' },
    });

    return {
      sucesso: true,
      total: viagens.length,
      viagens: viagens.map((viagem) => this.mapearViagem(viagem)),
    };
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroViagensDto) {
    this.validarIntervalosDoFiltro(filtro);

    const pagina = filtro.pagina ?? 1;
    const limite = filtro.limite ?? 30;
    const offset = (pagina - 1) * limite;

    const query = this.viagemRepository
      .createQueryBuilder('viagem')
      .where('viagem.idEmpresa = :idEmpresa', { idEmpresa: String(idEmpresa) });

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

    return {
      sucesso: true,
      pagina,
      limite,
      total,
      paginas: total > 0 ? Math.ceil(total / limite) : 0,
      viagens: viagens.map((viagem) => this.mapearViagem(viagem)),
    };
  }

  async buscarPorId(idEmpresa: number, idViagem: number) {
    const viagem = await this.buscarViagemPorIdOuFalhar(idEmpresa, idViagem);

    return {
      sucesso: true,
      viagem: this.mapearViagem(viagem),
    };
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
    });

    try {
      const viagem = await this.viagemRepository.save(
        this.viagemRepository.create({
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

      return {
        sucesso: true,
        mensagem: 'Viagem cadastrada com sucesso.',
        viagem: this.mapearViagem(viagem),
      };
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
    const viagemAtual = await this.buscarViagemPorIdOuFalhar(
      idEmpresa,
      idViagem,
    );
    const payload = this.normalizarAtualizacao(dados);

    const dataInicio = payload.dataInicio ?? viagemAtual.dataInicio;
    const dataFim =
      payload.dataFim !== undefined ? payload.dataFim : viagemAtual.dataFim;
    const kmInicial = payload.kmInicial ?? viagemAtual.kmInicial;
    const kmFinal =
      payload.kmFinal !== undefined ? payload.kmFinal : viagemAtual.kmFinal;

    this.validarConsistenciaDaViagem({
      dataInicio,
      dataFim,
      kmInicial,
      kmFinal,
    });

    try {
      await this.viagemRepository.update(
        { idViagem, idEmpresa: String(idEmpresa) },
        {
          ...payload,
          usuarioAtualizacao:
            payload.usuarioAtualizacao ??
            this.normalizarUsuarioAtualizacao(usuarioJwt.email),
        },
      );

      const viagemAtualizada = await this.buscarViagemPorIdOuFalhar(
        idEmpresa,
        idViagem,
      );

      return {
        sucesso: true,
        mensagem: 'Viagem atualizada com sucesso.',
        viagem: this.mapearViagem(viagemAtualizada),
      };
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  async remover(idEmpresa: number, idViagem: number) {
    const resultado = await this.viagemRepository.delete({
      idViagem,
      idEmpresa: String(idEmpresa),
    });

    if (!resultado.affected) {
      throw new NotFoundException(
        'Viagem nao encontrada para a empresa logada.',
      );
    }

    return {
      sucesso: true,
      mensagem: 'Viagem removida com sucesso.',
      idViagem,
    };
  }

  private async buscarViagemPorIdOuFalhar(idEmpresa: number, idViagem: number) {
    const viagem = await this.viagemRepository.findOne({
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

  private normalizarCriacao(dados: CriarViagemDto) {
    return {
      idVeiculo: dados.idVeiculo,
      idMotorista: dados.idMotorista,
      dataInicio: new Date(dados.dataInicio),
      dataFim: dados.dataFim ? new Date(dados.dataFim) : null,
      kmInicial: dados.kmInicial,
      kmFinal: dados.kmFinal ?? null,
      status: (dados.status ?? 'A').trim().toUpperCase(),
      observacao: dados.observacao?.trim() ? dados.observacao.trim() : null,
      valorFrete: dados.valorFrete ?? 0,
      media: dados.media ?? 0,
      totalDespesas: dados.totalDespesas ?? 0,
      totalAbastecimentos: dados.totalAbastecimentos ?? 0,
      totalKm: dados.totalKm ?? 0,
      totalLucro: dados.totalLucro ?? 0,
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

  private mapearViagem(viagem: ViagemEntity): ViagemNormalizada {
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
      valorFrete: this.converterNumero(viagem.valorFrete),
      media: this.converterNumero(viagem.media),
      totalDespesas: this.converterNumero(viagem.totalDespesas),
      totalAbastecimentos: this.converterNumero(viagem.totalAbastecimentos),
      totalKm: viagem.totalKm,
      totalLucro: this.converterNumero(viagem.totalLucro),
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
