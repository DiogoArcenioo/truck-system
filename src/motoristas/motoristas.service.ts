import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { EntityManager, QueryFailedError, Repository, SelectQueryBuilder } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { AtualizarMotoristaDto } from './dto/atualizar-motorista.dto';
import { CriarMotoristaDto } from './dto/criar-motorista.dto';
import { FiltroMotoristasDto } from './dto/filtro-motoristas.dto';
import { ListarMotoristaDto } from './dto/listar-motorista.dto';
import { MotoristaEntity } from './entities/motorista.entity';
import {
  CATEGORIAS_CNH_OPCOES,
  STATUS_MOTORISTA_LABEL_POR_CODIGO,
  STATUS_MOTORISTA_OPCOES,
} from './motoristas.constants';

@Injectable()
export class MotoristasService {
  constructor(
    @InjectRepository(MotoristaEntity)
    private readonly motoristaRepository: Repository<MotoristaEntity>,
  ) {}

  listarOpcoes() {
    return {
      sucesso: true,
      statusMotorista: STATUS_MOTORISTA_OPCOES,
      categoriasCnh: CATEGORIAS_CNH_OPCOES,
    };
  }

  async listarTodos(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (motoristaRepository) => {
      const motoristas = await motoristaRepository.find({
        where: { idEmpresa: String(idEmpresa) },
        order: { nome: 'ASC', idMotorista: 'ASC' },
      });

      const dados = motoristas.map((motorista) =>
        this.mapearMotorista(motorista),
      );

      return {
        sucesso: true,
        total: dados.length,
        motoristas: dados,
      };
    });
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroMotoristasDto) {
    this.validarIntervalosDoFiltro(filtro);

    return this.executarComRls(idEmpresa, async (motoristaRepository) => {
      const pagina = filtro.pagina ?? 1;
      const limite = filtro.limite ?? 20;
      const offset = (pagina - 1) * limite;
      const ordem = filtro.ordem ?? 'ASC';
      const colunaOrdenacao = this.resolverColunaOrdenacao(filtro.ordenarPor);

      const query = motoristaRepository
        .createQueryBuilder('motorista')
        .where('motorista.idEmpresa = :idEmpresa', { idEmpresa: String(idEmpresa) });

      this.aplicarFiltros(query, filtro);

      query
        .orderBy(`motorista.${colunaOrdenacao}`, ordem)
        .addOrderBy('motorista.idMotorista', 'ASC')
        .skip(offset)
        .take(limite);

      const [motoristas, total] = await query.getManyAndCount();
      const dados = motoristas.map((motorista) => this.mapearMotorista(motorista));

      return {
        sucesso: true,
        paginaAtual: pagina,
        limite,
        total,
        totalPaginas: total > 0 ? Math.ceil(total / limite) : 0,
        motoristas: dados,
      };
    });
  }

  async buscarPorId(idEmpresa: number, idMotorista: number) {
    return this.executarComRls(idEmpresa, async (motoristaRepository) => {
      const motorista = await motoristaRepository.findOne({
        where: { idEmpresa: String(idEmpresa), idMotorista },
      });

      if (!motorista) {
        throw new NotFoundException('Motorista nao encontrado para a empresa logada.');
      }

      return {
        sucesso: true,
        motorista: this.mapearMotorista(motorista),
      };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarMotoristaDto,
    _usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (motoristaRepository, manager) => {
        const idMotorista = await this.obterProximoIdMotorista(manager);

        const novo = motoristaRepository.create({
          idMotorista,
          idEmpresa: String(idEmpresa),
          nome: this.normalizarTexto(dados.nome),
          cpf: dados.cpf,
          cnh: this.normalizarTexto(dados.cnh),
          categoriaCnh: dados.categoriaCnh,
          validadeCnh: this.normalizarData(dados.validadeCnh),
          status: dados.status ?? 'A',
        });

        const motorista = await motoristaRepository.save(novo);

        return {
          sucesso: true,
          mensagem: 'Motorista cadastrado com sucesso.',
          motorista: this.mapearMotorista(motorista),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar');
    }
  }

  async atualizar(
    idEmpresa: number,
    idMotorista: number,
    dados: AtualizarMotoristaDto,
    _usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (motoristaRepository) => {
        const motorista = await motoristaRepository.findOne({
          where: { idEmpresa: String(idEmpresa), idMotorista },
        });

        if (!motorista) {
          throw new NotFoundException('Motorista nao encontrado para a empresa logada.');
        }

        if (dados.nome !== undefined) {
          motorista.nome = this.normalizarTexto(dados.nome);
        }

        if (dados.cpf !== undefined) {
          motorista.cpf = dados.cpf;
        }

        if (dados.cnh !== undefined) {
          motorista.cnh = this.normalizarTexto(dados.cnh);
        }

        if (dados.categoriaCnh !== undefined) {
          motorista.categoriaCnh = dados.categoriaCnh;
        }

        if (dados.validadeCnh !== undefined) {
          motorista.validadeCnh = this.normalizarData(dados.validadeCnh);
        }

        if (dados.status !== undefined) {
          motorista.status = dados.status;
        }

        const atualizado = await motoristaRepository.save(motorista);

        return {
          sucesso: true,
          mensagem: 'Motorista atualizado com sucesso.',
          motorista: this.mapearMotorista(atualizado),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (
      motoristaRepository: Repository<MotoristaEntity>,
      manager: EntityManager,
    ) => Promise<T>,
  ): Promise<T> {
    return this.motoristaRepository.manager.transaction(async (manager) => {
      await configurarContextoEmpresaRls(manager, idEmpresa);
      const motoristaRepository = manager.getRepository(MotoristaEntity);
      return callback(motoristaRepository, manager);
    });
  }

  private aplicarFiltros(
    query: SelectQueryBuilder<MotoristaEntity>,
    filtro: FiltroMotoristasDto,
  ) {
    if (filtro.idMotorista !== undefined) {
      query.andWhere('motorista.idMotorista = :idMotorista', {
        idMotorista: filtro.idMotorista,
      });
    }

    if (filtro.nome) {
      query.andWhere('motorista.nome ILIKE :nome', {
        nome: `%${filtro.nome}%`,
      });
    }

    if (filtro.cpf) {
      query.andWhere('motorista.cpf = :cpf', {
        cpf: filtro.cpf,
      });
    }

    if (filtro.cnh) {
      query.andWhere('motorista.cnh ILIKE :cnh', {
        cnh: `%${filtro.cnh}%`,
      });
    }

    if (filtro.status) {
      query.andWhere('motorista.status = :status', {
        status: filtro.status,
      });
    }

    if (filtro.categoriaCnh) {
      query.andWhere('motorista.categoriaCnh = :categoriaCnh', {
        categoriaCnh: filtro.categoriaCnh,
      });
    }

    if (filtro.texto?.trim()) {
      const texto = `%${filtro.texto.trim().toUpperCase()}%`;

      query.andWhere(
        '(UPPER(motorista.nome) LIKE :texto OR motorista.cpf LIKE :texto OR UPPER(motorista.cnh) LIKE :texto)',
        { texto },
      );
    }

    if (filtro.validadeDe) {
      query.andWhere('motorista.validadeCnh >= :validadeDe', {
        validadeDe: this.normalizarData(filtro.validadeDe),
      });
    }

    if (filtro.validadeAte) {
      query.andWhere('motorista.validadeCnh <= :validadeAte', {
        validadeAte: this.normalizarData(filtro.validadeAte),
      });
    }
  }

  private validarIntervalosDoFiltro(filtro: FiltroMotoristasDto) {
    if (
      filtro.validadeDe &&
      filtro.validadeAte &&
      new Date(filtro.validadeAte) < new Date(filtro.validadeDe)
    ) {
      throw new BadRequestException(
        'Filtro invalido: validadeAte deve ser maior ou igual a validadeDe.',
      );
    }
  }

  private resolverColunaOrdenacao(
    ordenarPor: FiltroMotoristasDto['ordenarPor'],
  ): keyof MotoristaEntity {
    if (ordenarPor === 'id_motorista') {
      return 'idMotorista';
    }
    if (ordenarPor === 'cpf') {
      return 'cpf';
    }
    if (ordenarPor === 'cnh') {
      return 'cnh';
    }
    if (ordenarPor === 'categoria_cnh') {
      return 'categoriaCnh';
    }
    if (ordenarPor === 'validade_cnh') {
      return 'validadeCnh';
    }
    if (ordenarPor === 'status') {
      return 'status';
    }

    return 'nome';
  }

  private async obterProximoIdMotorista(manager: EntityManager): Promise<number> {
    try {
      const rows = (await manager.query(
        "SELECT nextval(pg_get_serial_sequence('app.motoristas','id_motorista'))::int AS id",
      )) as Array<{ id?: number }>;

      const id = Number(rows[0]?.id ?? 0);
      if (Number.isFinite(id) && id > 0) {
        return id;
      }
    } catch {
      // fallback por max(id)+1 quando a sequence nao estiver disponivel
    }

    const rows = (await manager.query(
      'SELECT COALESCE(MAX(id_motorista), 0)::int + 1 AS id FROM app.motoristas',
    )) as Array<{ id?: number }>;

    const id = Number(rows[0]?.id ?? 0);
    if (!Number.isFinite(id) || id <= 0) {
      throw new BadRequestException(
        'Nao foi possivel determinar o proximo id de motorista.',
      );
    }

    return id;
  }

  private normalizarTexto(valor: string): string {
    return valor.trim().toUpperCase();
  }

  private normalizarData(valor: string): string {
    const data = new Date(valor);
    if (Number.isNaN(data.getTime())) {
      throw new BadRequestException('Data invalida informada para validade da CNH.');
    }

    return data.toISOString().slice(0, 10);
  }

  private mapearMotorista(motorista: MotoristaEntity): ListarMotoristaDto {
    const statusNormalizado = (motorista.status ?? '').trim().toUpperCase();
    const validadeBruta = motorista.validadeCnh as unknown;
    const validadeCnh =
      validadeBruta instanceof Date
        ? validadeBruta.toISOString().slice(0, 10)
        : `${validadeBruta}`;

    return {
      idMotorista: motorista.idMotorista,
      nome: this.normalizarTexto(motorista.nome),
      cpf: motorista.cpf?.trim(),
      cnh: this.normalizarTexto(motorista.cnh),
      categoriaCnh: this.normalizarTexto(motorista.categoriaCnh),
      validadeCnh,
      status: statusNormalizado,
      statusDescricao:
        STATUS_MOTORISTA_LABEL_POR_CODIGO[
          statusNormalizado as keyof typeof STATUS_MOTORISTA_LABEL_POR_CODIGO
        ] ?? statusNormalizado,
    };
  }

  private tratarErroPersistencia(error: unknown, acao: string): never {
    if (error instanceof BadRequestException || error instanceof NotFoundException) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as { code?: string };

      if (erroPg.code === '23505') {
        throw new BadRequestException(
          'CPF ou CNH ja cadastrado para esta empresa.',
        );
      }

      if (erroPg.code === '22007') {
        throw new BadRequestException('Data de validade da CNH invalida.');
      }

      if (erroPg.code === '42P01') {
        throw new BadRequestException('Tabela app.motoristas nao encontrada.');
      }
    }

    throw new BadRequestException(
      `Nao foi possivel ${acao} motorista neste momento.`,
    );
  }
}
