import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { AtualizarPneuDto } from './dto/atualizar-pneu.dto';
import { CriarPneuDto } from './dto/criar-pneu.dto';
import { FiltroMovimentacoesPneuDto } from './dto/filtro-movimentacoes-pneu.dto';
import { FiltroPneusDto } from './dto/filtro-pneus.dto';
import { MovimentarPneuDto } from './dto/movimentar-pneu.dto';

type RegistroBanco = Record<string, unknown>;

type StatusLocalPneu =
  | 'ESTOQUE'
  | 'EM_USO'
  | 'CONSERTO'
  | 'BAIXA'
  | 'DESCARTE';

type DestinoMovimentacao = StatusLocalPneu | 'VEICULO';

type PneuNormalizado = {
  idPneu: number;
  idEmpresa: number;
  numeroFogo: string;
  marca: string | null;
  modelo: string | null;
  medida: string | null;
  tipo: string | null;
  valor: number;
  statusLocal: StatusLocalPneu;
  ativo: boolean;
  observacoes: string | null;
  usuarioAtualizacao: string | null;
  criadoEm: Date | null;
  atualizadoEm: Date | null;
  idVeiculoAtual: number | null;
  placaVeiculoAtual: string | null;
  posicaoAtual: string | null;
};

type MovimentacaoNormalizada = {
  idMovimentacao: number;
  idPneu: number;
  numeroFogo: string | null;
  destino: DestinoMovimentacao;
  idVeiculoOrigem: number | null;
  placaVeiculoOrigem: string | null;
  posicaoOrigem: string | null;
  idVeiculoDestino: number | null;
  placaVeiculoDestino: string | null;
  posicaoDestino: string | null;
  motivo: string | null;
  observacoes: string | null;
  usuarioAtualizacao: string | null;
  dataMovimentacao: Date | null;
  criadoEm: Date | null;
};

type VinculoAtivoPneu = {
  idVinculo: number;
  idPneu: number;
  idVeiculo: number;
  posicao: string;
};

type VeiculoOpcao = {
  idVeiculo: number;
  placa: string;
  status: string | null;
};

type PosicaoPadrao = {
  codigo: string;
  label: string;
  eixo: string;
  ordem: number;
};

const STATUS_LOCAL_VALIDOS: StatusLocalPneu[] = [
  'ESTOQUE',
  'EM_USO',
  'CONSERTO',
  'BAIXA',
  'DESCARTE',
];

const DESTINOS_VALIDOS: DestinoMovimentacao[] = [
  'ESTOQUE',
  'CONSERTO',
  'BAIXA',
  'DESCARTE',
  'VEICULO',
];

const POSICOES_PADRAO: PosicaoPadrao[] = [
  { codigo: 'D1E', label: 'Dianteiro esquerdo', eixo: 'Dianteiro', ordem: 1 },
  { codigo: 'D1D', label: 'Dianteiro direito', eixo: 'Dianteiro', ordem: 2 },
  {
    codigo: 'T1E1',
    label: 'Traseiro 1 eixo esquerdo externo',
    eixo: 'Traseiro 1',
    ordem: 3,
  },
  {
    codigo: 'T1E2',
    label: 'Traseiro 1 eixo esquerdo interno',
    eixo: 'Traseiro 1',
    ordem: 4,
  },
  {
    codigo: 'T1D1',
    label: 'Traseiro 1 eixo direito interno',
    eixo: 'Traseiro 1',
    ordem: 5,
  },
  {
    codigo: 'T1D2',
    label: 'Traseiro 1 eixo direito externo',
    eixo: 'Traseiro 1',
    ordem: 6,
  },
  {
    codigo: 'T2E1',
    label: 'Traseiro 2 eixo esquerdo externo',
    eixo: 'Traseiro 2',
    ordem: 7,
  },
  {
    codigo: 'T2E2',
    label: 'Traseiro 2 eixo esquerdo interno',
    eixo: 'Traseiro 2',
    ordem: 8,
  },
  {
    codigo: 'T2D1',
    label: 'Traseiro 2 eixo direito interno',
    eixo: 'Traseiro 2',
    ordem: 9,
  },
  {
    codigo: 'T2D2',
    label: 'Traseiro 2 eixo direito externo',
    eixo: 'Traseiro 2',
    ordem: 10,
  },
  { codigo: 'ESTEPE', label: 'Estepe', eixo: 'Reserva', ordem: 11 },
];

@Injectable()
export class PneusService {
  private readonly logger = new Logger(PneusService.name);
  private estruturaInicializada = false;
  private inicializacaoEmAndamento: Promise<void> | null = null;
  private estruturaDisponivel = true;

  constructor(private readonly dataSource: DataSource) {}

  async listarComFiltro(idEmpresa: number, filtro: FiltroPneusDto) {
    this.validarIntervaloPaginacao(filtro);

    return this.executarComRls(idEmpresa, async (manager) => {
      const where: string[] = ['p.id_empresa = $1'];
      const valores: Array<number | string | boolean> = [idEmpresa];

      if (filtro.idPneu !== undefined) {
        valores.push(filtro.idPneu);
        where.push(`p.id_pneu = $${valores.length}`);
      }

      if (filtro.idVeiculo !== undefined) {
        valores.push(filtro.idVeiculo);
        where.push(`pv.id_veiculo = $${valores.length}`);
      }

      const situacao = this.normalizarSituacaoFiltro(filtro.situacao);
      if (situacao !== 'TODOS') {
        valores.push(situacao === 'ATIVO');
        where.push(`p.ativo = $${valores.length}`);
      }

      const statusLocal = this.normalizarStatusLocalFiltro(filtro.statusLocal);
      if (statusLocal !== 'TODOS') {
        valores.push(statusLocal);
        where.push(`p.status_local = $${valores.length}`);
      }

      if (filtro.texto?.trim()) {
        valores.push(`%${filtro.texto.trim()}%`);
        where.push(
          `(
            p.numero_fogo ILIKE $${valores.length}
            OR COALESCE(p.marca, '') ILIKE $${valores.length}
            OR COALESCE(p.modelo, '') ILIKE $${valores.length}
            OR COALESCE(p.medida, '') ILIKE $${valores.length}
            OR COALESCE(p.tipo, '') ILIKE $${valores.length}
            OR COALESCE(v.placa, '') ILIKE $${valores.length}
            OR COALESCE(pv.posicao, '') ILIKE $${valores.length}
          )`,
        );
      }

      const whereSql = where.length > 0 ? `WHERE ${where.join(' AND ')}` : '';
      const pagina = filtro.pagina ?? 1;
      const limite = filtro.limite ?? 120;
      const offset = (pagina - 1) * limite;
      const ordem = filtro.ordem ?? 'DESC';
      const ordenarPor = this.resolverColunaOrdenacao(filtro.ordenarPor);

      const sqlBase = `
        FROM app.pneus p
        LEFT JOIN app.pneu_vinculos_veiculo pv
          ON pv.id_empresa = p.id_empresa
         AND pv.id_pneu = p.id_pneu
         AND pv.ativo = true
        LEFT JOIN app.veiculo v
          ON CAST(v.id_empresa AS TEXT) = CAST(p.id_empresa AS TEXT)
         AND v.id_veiculo = pv.id_veiculo
        ${whereSql}
      `;

      const countRows = (await manager.query(
        `SELECT COUNT(1)::int AS total ${sqlBase}`,
        valores,
      )) as Array<{ total?: unknown }>;
      const rows = (await manager.query(
        `
        SELECT
          p.*,
          pv.id_veiculo AS id_veiculo_atual,
          pv.posicao AS posicao_atual,
          v.placa AS placa_veiculo_atual
        ${sqlBase}
        ORDER BY ${ordenarPor} ${ordem}, p.id_pneu DESC
        LIMIT $${valores.length + 1}
        OFFSET $${valores.length + 2}
        `,
        [...valores, limite, offset],
      )) as RegistroBanco[];

      const total = this.toNumber(countRows[0]?.total) ?? 0;
      return {
        sucesso: true,
        paginaAtual: pagina,
        limite,
        total,
        totalPaginas: total > 0 ? Math.ceil(total / limite) : 0,
        pneus: rows.map((row) => this.mapearPneu(row)),
      };
    });
  }

  async buscarPorId(idEmpresa: number, idPneu: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const pneu = await this.buscarPneuDetalhadoOuFalhar(manager, idEmpresa, idPneu);
      return { sucesso: true, pneu };
    });
  }

  async listarOpcoes(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const [veiculosRows, resumoRows] = await Promise.all([
        manager.query(
          `
          SELECT
            v.id_veiculo,
            v.placa,
            COALESCE(
              NULLIF(UPPER(TRIM(COALESCE(to_jsonb(v)->>'status', ''))), ''),
              NULLIF(UPPER(TRIM(COALESCE(to_jsonb(v)->>'situacao', ''))), ''),
              CASE
                WHEN LOWER(COALESCE(to_jsonb(v)->>'ativo', '')) IN ('true', 't', '1') THEN 'A'
                WHEN LOWER(COALESCE(to_jsonb(v)->>'ativo', '')) IN ('false', 'f', '0') THEN 'I'
                ELSE NULL
              END
            ) AS status
          FROM app.veiculo v
          WHERE CAST(v.id_empresa AS TEXT) = $1
          ORDER BY v.placa ASC
          `,
          [String(idEmpresa)],
        ) as Promise<RegistroBanco[]>,
        manager.query(
          `
          SELECT status_local, COUNT(1)::int AS total
          FROM app.pneus
          WHERE id_empresa = $1
            AND ativo = true
          GROUP BY status_local
          `,
          [String(idEmpresa)],
        ) as Promise<RegistroBanco[]>,
      ]);

      const resumo: Record<StatusLocalPneu, number> = {
        ESTOQUE: 0,
        EM_USO: 0,
        CONSERTO: 0,
        BAIXA: 0,
        DESCARTE: 0,
      };

      for (const row of resumoRows) {
        const status = this.normalizarStatusLocal(this.toText(row.status_local));
        if (status) resumo[status] = this.toNumber(row.total) ?? 0;
      }

      const veiculos = veiculosRows
        .map<VeiculoOpcao | null>((row) => {
          const idVeiculo = this.toNumber(row.id_veiculo);
          const placa = this.toText(row.placa);
          if (!idVeiculo || !placa) return null;
          return { idVeiculo, placa, status: this.toText(row.status) };
        })
        .filter((item): item is VeiculoOpcao => Boolean(item));

      return {
        sucesso: true,
        statusLocalDisponiveis: STATUS_LOCAL_VALIDOS,
        destinosMovimentacao: DESTINOS_VALIDOS,
        posicoesPadrao: POSICOES_PADRAO,
        resumoStatus: resumo,
        veiculos,
      };
    });
  }

  async listarMovimentacoes(idEmpresa: number, filtro: FiltroMovimentacoesPneuDto) {
    this.validarPeriodoMovimentacao(filtro);

    return this.executarComRls(idEmpresa, async (manager) => {
      const where: string[] = ['m.id_empresa = $1'];
      const valores: Array<string | number> = [idEmpresa];

      if (filtro.idPneu !== undefined) {
        valores.push(filtro.idPneu);
        where.push(`m.id_pneu = $${valores.length}`);
      }

      if (filtro.idVeiculo !== undefined) {
        valores.push(filtro.idVeiculo);
        where.push(
          `(m.id_veiculo_origem = $${valores.length} OR m.id_veiculo_destino = $${valores.length})`,
        );
      }

      if (filtro.destino) {
        valores.push(filtro.destino);
        where.push(`m.destino = $${valores.length}`);
      }

      if (filtro.dataDe) {
        valores.push(filtro.dataDe);
        where.push(`m.data_movimentacao >= $${valores.length}`);
      }

      if (filtro.dataAte) {
        valores.push(filtro.dataAte);
        where.push(`m.data_movimentacao <= $${valores.length}`);
      }

      const whereSql = where.length > 0 ? `WHERE ${where.join(' AND ')}` : '';
      const limite = filtro.limite ?? 120;

      const rows = (await manager.query(
        `
        SELECT
          m.*,
          p.numero_fogo,
          vo.placa AS placa_veiculo_origem,
          vd.placa AS placa_veiculo_destino
        FROM app.pneu_movimentacoes m
        LEFT JOIN app.pneus p
          ON CAST(p.id_empresa AS TEXT) = CAST(m.id_empresa AS TEXT)
         AND p.id_pneu = m.id_pneu
        LEFT JOIN app.veiculo vo
          ON CAST(vo.id_empresa AS TEXT) = CAST(m.id_empresa AS TEXT)
         AND vo.id_veiculo = m.id_veiculo_origem
        LEFT JOIN app.veiculo vd
          ON CAST(vd.id_empresa AS TEXT) = CAST(m.id_empresa AS TEXT)
         AND vd.id_veiculo = m.id_veiculo_destino
        ${whereSql}
        ORDER BY m.data_movimentacao DESC, m.id_movimentacao DESC
        LIMIT $${valores.length + 1}
        `,
        [...valores, limite],
      )) as RegistroBanco[];

      return {
        sucesso: true,
        total: rows.length,
        movimentacoes: rows.map((row) => this.mapearMovimentacao(row)),
      };
    });
  }

  async cadastrar(idEmpresa: number, dados: CriarPneuDto, usuarioJwt: JwtUsuarioPayload) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => {
        const usuarioAtualizacao = this.normalizarUsuario(
          dados.usuarioAtualizacao ?? usuarioJwt.email,
        );
        const numeroFogo = this.normalizarNumeroFogo(dados.numeroFogo);
        const statusLocalSolicitado = this.normalizarStatusLocal(dados.statusLocal) ?? 'ESTOQUE';
        const idVeiculo = dados.idVeiculo ?? null;
        const posicao = this.normalizarPosicao(dados.posicao);

        if (idVeiculo !== null && !posicao) {
          throw new BadRequestException('Informe a posicao para vincular o pneu ao veiculo.');
        }

        if (statusLocalSolicitado === 'EM_USO' && idVeiculo === null) {
          throw new BadRequestException(
            'Para cadastrar como EM_USO, informe o veiculo e a posicao de montagem.',
          );
        }

        await this.validarNumeroFogoUnico(manager, idEmpresa, numeroFogo);

        const statusPersistido: StatusLocalPneu =
          idVeiculo !== null ? 'EM_USO' : statusLocalSolicitado;

        const insertRows = (await manager.query(
          `
          INSERT INTO app.pneus (
            id_empresa,
            numero_fogo,
            marca,
            modelo,
            medida,
            tipo,
            valor,
            status_local,
            ativo,
            observacoes,
            usuario_atualizacao,
            criado_em,
            atualizado_em
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE, $9, $10, NOW(), NOW())
          RETURNING id_pneu
          `,
          [
            idEmpresa,
            numeroFogo,
            this.normalizarTextoOpcional(dados.marca),
            this.normalizarTextoOpcional(dados.modelo),
            this.normalizarTextoOpcional(dados.medida),
            this.normalizarTextoOpcional(dados.tipo),
            dados.valor ?? 0,
            statusPersistido,
            this.normalizarTextoOpcional(dados.observacoes),
            usuarioAtualizacao,
          ],
        )) as Array<{ id_pneu?: unknown }>;

        const idPneu = this.toNumber(insertRows[0]?.id_pneu);
        if (!idPneu) {
          throw new BadRequestException('Falha ao cadastrar pneu.');
        }

        if (idVeiculo !== null && posicao) {
          await this.validarVeiculoExiste(manager, idEmpresa, idVeiculo);
          await this.validarPosicaoLivre(manager, idEmpresa, idVeiculo, posicao, null);
          await this.inserirVinculoAtivo(
            manager,
            idEmpresa,
            idPneu,
            idVeiculo,
            posicao,
            usuarioAtualizacao,
          );
          await this.registrarMovimentacao(manager, idEmpresa, {
            idPneu,
            destino: 'VEICULO',
            idVeiculoOrigem: null,
            posicaoOrigem: null,
            idVeiculoDestino: idVeiculo,
            posicaoDestino: posicao,
            motivo: 'VINCULO_INICIAL',
            observacoes: 'Pneu vinculado no cadastro.',
            usuarioAtualizacao,
            dataMovimentacao: new Date(),
          });
        }

        const pneu = await this.buscarPneuDetalhadoOuFalhar(manager, idEmpresa, idPneu);
        return { sucesso: true, mensagem: 'Pneu cadastrado com sucesso.', pneu };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar');
    }
  }

  async atualizar(
    idEmpresa: number,
    idPneu: number,
    dados: AtualizarPneuDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => {
        const atual = await this.buscarPneuDetalhadoOuFalhar(manager, idEmpresa, idPneu);
        const vinculoAtivo = await this.buscarVinculoAtivoPneu(manager, idEmpresa, idPneu);

        const numeroFogo =
          dados.numeroFogo !== undefined ? this.normalizarNumeroFogo(dados.numeroFogo) : atual.numeroFogo;
        const marca = dados.marca !== undefined ? this.normalizarTextoOpcional(dados.marca) : atual.marca;
        const modelo = dados.modelo !== undefined ? this.normalizarTextoOpcional(dados.modelo) : atual.modelo;
        const medida = dados.medida !== undefined ? this.normalizarTextoOpcional(dados.medida) : atual.medida;
        const tipo = dados.tipo !== undefined ? this.normalizarTextoOpcional(dados.tipo) : atual.tipo;
        const valor = dados.valor !== undefined ? dados.valor : atual.valor;
        const ativo = dados.ativo !== undefined ? dados.ativo : atual.ativo;
        const observacoes =
          dados.observacoes !== undefined
            ? this.normalizarTextoOpcional(dados.observacoes)
            : atual.observacoes;
        const statusLocal =
          dados.statusLocal !== undefined ? this.normalizarStatusLocal(dados.statusLocal) : atual.statusLocal;
        const usuarioAtualizacao = this.normalizarUsuario(dados.usuarioAtualizacao ?? usuarioJwt.email);

        if (!statusLocal) {
          throw new BadRequestException('statusLocal invalido.');
        }

        if (statusLocal === 'EM_USO' && !vinculoAtivo) {
          throw new BadRequestException(
            'Este pneu nao possui vinculo ativo com veiculo. Use a aba de movimentacao para montar.',
          );
        }

        if (statusLocal !== 'EM_USO' && vinculoAtivo) {
          throw new BadRequestException(
            'Este pneu esta vinculado a um veiculo. Remova o vinculo na aba de movimentacao antes de alterar o status.',
          );
        }

        if (!ativo && vinculoAtivo) {
          throw new BadRequestException('Nao e possivel inativar pneu vinculado a veiculo.');
        }

        if (numeroFogo !== atual.numeroFogo) {
          await this.validarNumeroFogoUnico(manager, idEmpresa, numeroFogo, idPneu);
        }

        await manager.query(
          `
          UPDATE app.pneus
          SET
            numero_fogo = $1,
            marca = $2,
            modelo = $3,
            medida = $4,
            tipo = $5,
            valor = $6,
            status_local = $7,
            ativo = $8,
            observacoes = $9,
            usuario_atualizacao = $10,
            atualizado_em = NOW()
          WHERE id_empresa = $11
            AND id_pneu = $12
          `,
          [
            numeroFogo,
            marca,
            modelo,
            medida,
            tipo,
            valor,
            statusLocal,
            ativo,
            observacoes,
            usuarioAtualizacao,
            idEmpresa,
            idPneu,
          ],
        );

        const pneu = await this.buscarPneuDetalhadoOuFalhar(manager, idEmpresa, idPneu);
        return { sucesso: true, mensagem: 'Pneu atualizado com sucesso.', pneu };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  async remover(idEmpresa: number, idPneu: number, usuarioJwt: JwtUsuarioPayload) {
    return this.executarComRls(idEmpresa, async (manager) => {
      await this.buscarPneuDetalhadoOuFalhar(manager, idEmpresa, idPneu);
      const vinculoAtivo = await this.buscarVinculoAtivoPneu(manager, idEmpresa, idPneu);
      if (vinculoAtivo) {
        throw new BadRequestException('Pneu vinculado a veiculo. Remova o vinculo antes de inativar.');
      }

      await manager.query(
        `
        UPDATE app.pneus
        SET
          ativo = false,
          usuario_atualizacao = $1,
          atualizado_em = NOW()
        WHERE id_empresa = $2
          AND id_pneu = $3
          AND ativo = true
        `,
        [this.normalizarUsuario(usuarioJwt.email), idEmpresa, idPneu],
      );

      return { sucesso: true, mensagem: 'Pneu inativado com sucesso.', idPneu };
    });
  }

  async movimentar(idEmpresa: number, dados: MovimentarPneuDto, usuarioJwt: JwtUsuarioPayload) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => {
        const destino = this.normalizarDestino(dados.destino);
        if (!destino) {
          throw new BadRequestException('Destino de movimentacao invalido.');
        }

        const usuarioAtualizacao = this.normalizarUsuario(dados.usuarioAtualizacao ?? usuarioJwt.email);
        const dataMovimentacao = dados.dataMovimentacao ? new Date(dados.dataMovimentacao) : new Date();
        if (Number.isNaN(dataMovimentacao.getTime())) {
          throw new BadRequestException('Data de movimentacao invalida.');
        }

        const pneu = await this.buscarPneuDetalhadoOuFalhar(manager, idEmpresa, dados.idPneu);
        if (!pneu.ativo) {
          throw new BadRequestException('Pneu inativo nao pode ser movimentado.');
        }

        const vinculoAtivo = await this.buscarVinculoAtivoPneu(
          manager,
          idEmpresa,
          dados.idPneu,
        );
        const idVeiculoOrigem = vinculoAtivo?.idVeiculo ?? null;
        let posicaoOrigem = vinculoAtivo?.posicao ?? null;
        if (!idVeiculoOrigem) {
          posicaoOrigem = pneu.statusLocal;
        }

        let idVeiculoDestino: number | null = null;
        let posicaoDestino: string | null = null;
        const statusLocalDestino: StatusLocalPneu = destino === 'VEICULO' ? 'EM_USO' : destino;

        if (destino === 'VEICULO') {
          const idVeiculo = dados.idVeiculoDestino ?? null;
          const posicao = this.normalizarPosicao(dados.posicaoDestino);

          if (idVeiculo === null || !posicao) {
            throw new BadRequestException('Para mover para veiculo, informe o veiculo e a posicao.');
          }

          await this.validarVeiculoExiste(manager, idEmpresa, idVeiculo);
          await this.validarPosicaoLivre(manager, idEmpresa, idVeiculo, posicao, dados.idPneu);
          await this.desativarVinculosAtivosPneu(manager, idEmpresa, dados.idPneu);
          await this.inserirVinculoAtivo(
            manager,
            idEmpresa,
            dados.idPneu,
            idVeiculo,
            posicao,
            usuarioAtualizacao,
          );

          idVeiculoDestino = idVeiculo;
          posicaoDestino = posicao;
        } else {
          await this.desativarVinculosAtivosPneu(manager, idEmpresa, dados.idPneu);
        }

        await manager.query(
          `
          UPDATE app.pneus
          SET
            status_local = $1,
            usuario_atualizacao = $2,
            atualizado_em = NOW()
          WHERE id_empresa = $3
            AND id_pneu = $4
          `,
          [statusLocalDestino, usuarioAtualizacao, idEmpresa, dados.idPneu],
        );

        await this.registrarMovimentacao(manager, idEmpresa, {
          idPneu: dados.idPneu,
          destino,
          idVeiculoOrigem,
          posicaoOrigem,
          idVeiculoDestino,
          posicaoDestino,
          motivo: this.normalizarTextoOpcional(dados.motivo),
          observacoes: this.normalizarTextoOpcional(dados.observacoes),
          usuarioAtualizacao,
          dataMovimentacao,
        });

        const pneuAtualizado = await this.buscarPneuDetalhadoOuFalhar(manager, idEmpresa, dados.idPneu);
        return {
          sucesso: true,
          mensagem:
            destino === 'VEICULO'
              ? 'Pneu vinculado ao veiculo com sucesso.'
              : `Pneu movido para ${destino.toLowerCase()} com sucesso.`,
          pneu: pneuAtualizado,
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'movimentar');
    }
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (manager: EntityManager) => Promise<T>,
  ): Promise<T> {
    await this.garantirEstrutura();
    this.exigirEstruturaDisponivel();

    return this.dataSource.manager.transaction(async (manager) => {
      await configurarContextoEmpresaRls(manager, idEmpresa);
      return callback(manager);
    });
  }

  private async buscarPneuDetalhadoOuFalhar(
    manager: EntityManager,
    idEmpresa: number,
    idPneu: number,
  ): Promise<PneuNormalizado> {
    const rows = (await manager.query(
      `
      SELECT
        p.*,
        pv.id_veiculo AS id_veiculo_atual,
        pv.posicao AS posicao_atual,
        v.placa AS placa_veiculo_atual
      FROM app.pneus p
      LEFT JOIN app.pneu_vinculos_veiculo pv
        ON pv.id_empresa = p.id_empresa
       AND pv.id_pneu = p.id_pneu
       AND pv.ativo = true
      LEFT JOIN app.veiculo v
        ON CAST(v.id_empresa AS TEXT) = CAST(p.id_empresa AS TEXT)
       AND v.id_veiculo = pv.id_veiculo
      WHERE p.id_empresa = $1
        AND p.id_pneu = $2
      LIMIT 1
      `,
      [idEmpresa, idPneu],
    )) as RegistroBanco[];

    const row = rows[0];
    if (!row) {
      throw new NotFoundException('Pneu nao encontrado para a empresa logada.');
    }

    return this.mapearPneu(row);
  }

  private async buscarVinculoAtivoPneu(
    manager: EntityManager,
    idEmpresa: number,
    idPneu: number,
  ): Promise<VinculoAtivoPneu | null> {
    const rows = (await manager.query(
      `
      SELECT id_vinculo, id_pneu, id_veiculo, posicao
      FROM app.pneu_vinculos_veiculo
      WHERE id_empresa = $1
        AND id_pneu = $2
        AND ativo = true
      LIMIT 1
      `,
      [idEmpresa, idPneu],
    )) as RegistroBanco[];

    const row = rows[0];
    if (!row) return null;

    const idVinculo = this.toNumber(row.id_vinculo);
    const idPneuOut = this.toNumber(row.id_pneu);
    const idVeiculo = this.toNumber(row.id_veiculo);
    const posicao = this.toText(row.posicao);
    if (!idVinculo || !idPneuOut || !idVeiculo || !posicao) return null;

    return { idVinculo, idPneu: idPneuOut, idVeiculo, posicao };
  }

  private async validarNumeroFogoUnico(
    manager: EntityManager,
    idEmpresa: number,
    numeroFogo: string,
    idPneuIgnorar?: number,
  ) {
    const valores: Array<string | number> = [idEmpresa, numeroFogo];
    const filtros = ['id_empresa = $1', 'numero_fogo = $2'];
    if (idPneuIgnorar !== undefined) {
      valores.push(idPneuIgnorar);
      filtros.push(`id_pneu <> $${valores.length}`);
    }

    const rows = (await manager.query(
      `SELECT id_pneu FROM app.pneus WHERE ${filtros.join(' AND ')} LIMIT 1`,
      valores,
    )) as RegistroBanco[];

    if (rows.length > 0) {
      throw new BadRequestException(`Ja existe um pneu com numero de fogo ${numeroFogo}.`);
    }
  }

  private async validarVeiculoExiste(
    manager: EntityManager,
    idEmpresa: number,
    idVeiculo: number,
  ) {
    const rows = (await manager.query(
      `
      SELECT id_veiculo
      FROM app.veiculo
      WHERE CAST(id_empresa AS TEXT) = $1
        AND id_veiculo = $2
      LIMIT 1
      `,
      [String(idEmpresa), idVeiculo],
    )) as RegistroBanco[];

    if (!rows[0]) {
      throw new BadRequestException(`Veiculo #${idVeiculo} nao encontrado para a empresa logada.`);
    }
  }

  private async validarPosicaoLivre(
    manager: EntityManager,
    idEmpresa: number,
    idVeiculo: number,
    posicao: string,
    idPneuIgnorar: number | null,
  ) {
    const valores: Array<string | number> = [idEmpresa, idVeiculo, posicao];
    const filtros = [
      'pv.id_empresa = $1',
      'pv.id_veiculo = $2',
      'pv.posicao = $3',
      'pv.ativo = true',
    ];

    if (idPneuIgnorar !== null) {
      valores.push(idPneuIgnorar);
      filtros.push(`pv.id_pneu <> $${valores.length}`);
    }

    const rows = (await manager.query(
      `
      SELECT pv.id_pneu, p.numero_fogo
      FROM app.pneu_vinculos_veiculo pv
      LEFT JOIN app.pneus p
        ON p.id_empresa = pv.id_empresa
       AND p.id_pneu = pv.id_pneu
      WHERE ${filtros.join(' AND ')}
      LIMIT 1
      `,
      valores,
    )) as RegistroBanco[];

    if (!rows[0]) return;

    const numeroFogo = this.toText(rows[0].numero_fogo);
    throw new BadRequestException(
      numeroFogo
        ? `A posicao ${posicao} ja esta ocupada pelo pneu ${numeroFogo}.`
        : `A posicao ${posicao} ja esta ocupada por outro pneu.`,
    );
  }

  private async desativarVinculosAtivosPneu(
    manager: EntityManager,
    idEmpresa: number,
    idPneu: number,
  ) {
    await manager.query(
      `
      UPDATE app.pneu_vinculos_veiculo
      SET
        ativo = false,
        atualizado_em = NOW()
      WHERE id_empresa = $1
        AND id_pneu = $2
        AND ativo = true
      `,
      [idEmpresa, idPneu],
    );
  }

  private async inserirVinculoAtivo(
    manager: EntityManager,
    idEmpresa: number,
    idPneu: number,
    idVeiculo: number,
    posicao: string,
    usuarioAtualizacao: string,
  ) {
    await manager.query(
      `
      INSERT INTO app.pneu_vinculos_veiculo (
        id_empresa,
        id_pneu,
        id_veiculo,
        posicao,
        ativo,
        usuario_atualizacao,
        criado_em,
        atualizado_em
      )
      VALUES ($1, $2, $3, $4, true, $5, NOW(), NOW())
      `,
      [idEmpresa, idPneu, idVeiculo, posicao, usuarioAtualizacao],
    );
  }

  private async registrarMovimentacao(
    manager: EntityManager,
    idEmpresa: number,
    dados: {
      idPneu: number;
      destino: DestinoMovimentacao;
      idVeiculoOrigem: number | null;
      posicaoOrigem: string | null;
      idVeiculoDestino: number | null;
      posicaoDestino: string | null;
      motivo: string | null;
      observacoes: string | null;
      usuarioAtualizacao: string;
      dataMovimentacao: Date;
    },
  ) {
    await manager.query(
      `
      INSERT INTO app.pneu_movimentacoes (
        id_empresa,
        id_pneu,
        id_veiculo_origem,
        posicao_origem,
        destino,
        id_veiculo_destino,
        posicao_destino,
        motivo,
        observacoes,
        usuario_atualizacao,
        data_movimentacao,
        criado_em
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
      `,
      [
        idEmpresa,
        dados.idPneu,
        dados.idVeiculoOrigem,
        dados.posicaoOrigem,
        dados.destino,
        dados.idVeiculoDestino,
        dados.posicaoDestino,
        dados.motivo,
        dados.observacoes,
        dados.usuarioAtualizacao,
        dados.dataMovimentacao.toISOString(),
      ],
    );
  }

  private resolverColunaOrdenacao(ordenarPor: FiltroPneusDto['ordenarPor']): string {
    if (ordenarPor === 'numero_fogo') return 'p.numero_fogo';
    if (ordenarPor === 'valor') return 'p.valor';
    if (ordenarPor === 'status_local') return 'p.status_local';
    if (ordenarPor === 'criado_em') return 'p.criado_em';
    if (ordenarPor === 'atualizado_em') return 'p.atualizado_em';
    return 'p.id_pneu';
  }

  private mapearPneu(row: RegistroBanco): PneuNormalizado {
    const statusLocal = this.normalizarStatusLocal(this.toText(row.status_local));
    if (!statusLocal) {
      throw new BadRequestException(
        'Tabela app.pneus com status_local invalido. Ajuste os dados cadastrados.',
      );
    }

    return {
      idPneu: this.toNumber(row.id_pneu) ?? 0,
      idEmpresa: this.toNumber(row.id_empresa) ?? 0,
      numeroFogo: this.toText(row.numero_fogo) ?? '',
      marca: this.toText(row.marca),
      modelo: this.toText(row.modelo),
      medida: this.toText(row.medida),
      tipo: this.toText(row.tipo),
      valor: this.toNumber(row.valor) ?? 0,
      statusLocal,
      ativo: this.toBoolean(row.ativo) ?? true,
      observacoes: this.toText(row.observacoes),
      usuarioAtualizacao: this.toText(row.usuario_atualizacao),
      criadoEm: this.toDate(row.criado_em),
      atualizadoEm: this.toDate(row.atualizado_em),
      idVeiculoAtual: this.toNumber(row.id_veiculo_atual),
      placaVeiculoAtual: this.toText(row.placa_veiculo_atual),
      posicaoAtual: this.toText(row.posicao_atual),
    };
  }

  private mapearMovimentacao(row: RegistroBanco): MovimentacaoNormalizada {
    const destino = this.normalizarDestino(this.toText(row.destino)) ?? 'ESTOQUE';
    return {
      idMovimentacao: this.toNumber(row.id_movimentacao) ?? 0,
      idPneu: this.toNumber(row.id_pneu) ?? 0,
      numeroFogo: this.toText(row.numero_fogo),
      destino,
      idVeiculoOrigem: this.toNumber(row.id_veiculo_origem),
      placaVeiculoOrigem: this.toText(row.placa_veiculo_origem),
      posicaoOrigem: this.toText(row.posicao_origem),
      idVeiculoDestino: this.toNumber(row.id_veiculo_destino),
      placaVeiculoDestino: this.toText(row.placa_veiculo_destino),
      posicaoDestino: this.toText(row.posicao_destino),
      motivo: this.toText(row.motivo),
      observacoes: this.toText(row.observacoes),
      usuarioAtualizacao: this.toText(row.usuario_atualizacao),
      dataMovimentacao: this.toDate(row.data_movimentacao),
      criadoEm: this.toDate(row.criado_em),
    };
  }

  private normalizarNumeroFogo(valor: string) {
    const numero = valor.trim().toUpperCase();
    if (!numero) throw new BadRequestException('Numero de fogo e obrigatorio.');
    if (numero.length > 60) {
      throw new BadRequestException('Numero de fogo excede o limite de 60 caracteres.');
    }
    return numero;
  }

  private normalizarUsuario(valor: string) {
    const usuario = (valor ?? '').trim().toUpperCase();
    if (!usuario || usuario.length < 2) {
      throw new BadRequestException('UsuarioAtualizacao invalido.');
    }
    return usuario.slice(0, 120);
  }

  private normalizarTextoOpcional(valor: string | null | undefined) {
    if (typeof valor !== 'string') return null;
    const texto = valor.trim();
    return texto.length > 0 ? texto.toUpperCase() : null;
  }

  private normalizarStatusLocal(valor: string | null | undefined): StatusLocalPneu | null {
    if (!valor) return null;
    const normalizado = valor.trim().toUpperCase();
    return STATUS_LOCAL_VALIDOS.includes(normalizado as StatusLocalPneu)
      ? (normalizado as StatusLocalPneu)
      : null;
  }

  private normalizarStatusLocalFiltro(valor: string | null | undefined): StatusLocalPneu | 'TODOS' {
    if (!valor) return 'TODOS';
    const normalizado = valor.trim().toUpperCase();
    if (normalizado === 'TODOS') return 'TODOS';
    const status = this.normalizarStatusLocal(normalizado);
    return status ?? 'TODOS';
  }

  private normalizarDestino(valor: string | null | undefined): DestinoMovimentacao | null {
    if (!valor) return null;
    const normalizado = valor.trim().toUpperCase();
    return DESTINOS_VALIDOS.includes(normalizado as DestinoMovimentacao)
      ? (normalizado as DestinoMovimentacao)
      : null;
  }

  private normalizarSituacaoFiltro(valor: string | null | undefined): 'ATIVO' | 'INATIVO' | 'TODOS' {
    if (!valor) return 'ATIVO';
    const normalizado = valor.trim().toUpperCase();
    if (normalizado === 'ATIVO' || normalizado === 'INATIVO' || normalizado === 'TODOS') {
      return normalizado;
    }
    return 'ATIVO';
  }

  private normalizarPosicao(valor: string | null | undefined): string | null {
    if (!valor) return null;
    const posicao = valor.trim().toUpperCase();
    return posicao.length > 0 ? posicao.slice(0, 30) : null;
  }

  private validarIntervaloPaginacao(filtro: FiltroPneusDto) {
    if ((filtro.pagina ?? 1) < 1) {
      throw new BadRequestException('Pagina deve ser maior ou igual a 1.');
    }
    const limite = filtro.limite ?? 120;
    if (limite < 1 || limite > 200) {
      throw new BadRequestException('Limite deve estar entre 1 e 200.');
    }
  }

  private validarPeriodoMovimentacao(filtro: FiltroMovimentacoesPneuDto) {
    if (filtro.dataDe && filtro.dataAte && new Date(filtro.dataAte) < new Date(filtro.dataDe)) {
      throw new BadRequestException('Filtro invalido: dataAte deve ser maior ou igual a dataDe.');
    }
  }

  private toNumber(value: unknown): number | null {
    if (value === null || value === undefined) return null;
    const numero = Number(value);
    return Number.isFinite(numero) ? numero : null;
  }

  private toText(value: unknown): string | null {
    if (typeof value !== 'string') return null;
    const texto = value.trim();
    return texto.length > 0 ? texto : null;
  }

  private toBoolean(value: unknown): boolean | null {
    if (typeof value === 'boolean') return value;
    if (typeof value === 'number') {
      if (value === 1) return true;
      if (value === 0) return false;
    }
    if (typeof value === 'string') {
      const normalizado = value.trim().toLowerCase();
      if (normalizado === 'true' || normalizado === 't' || normalizado === '1') return true;
      if (normalizado === 'false' || normalizado === 'f' || normalizado === '0') return false;
    }
    return null;
  }

  private toDate(value: unknown): Date | null {
    if (value === null || value === undefined) return null;
    const data = new Date(
      value instanceof Date || typeof value === 'string' || typeof value === 'number'
        ? value
        : '',
    );
    return Number.isNaN(data.getTime()) ? null : data;
  }

  private async garantirEstrutura() {
    if (this.estruturaInicializada) return;

    if (this.inicializacaoEmAndamento) {
      await this.inicializacaoEmAndamento;
      return;
    }

    this.inicializacaoEmAndamento = (async () => {
      try {
        const estruturaJaExiste = await this.verificarEstruturaPneusExiste();
        if (estruturaJaExiste) {
          this.estruturaDisponivel = true;
          this.estruturaInicializada = true;
          return;
        }

        try {
          await this.executarScriptCriacaoEstruturaPneus();
        } catch (errorCriacao) {
          const erroPg =
            errorCriacao instanceof QueryFailedError
              ? (errorCriacao.driverError as { code?: string; message?: string })
              : null;
          const estruturaDisponivelAposFalha = await this.verificarEstruturaPneusExiste();
          if (estruturaDisponivelAposFalha) {
            this.logger.warn(
              `Estrutura de pneus ja existe no banco, ignorando erro de criacao automatica. code=${erroPg?.code ?? 'N/A'} message=${erroPg?.message ?? (errorCriacao instanceof Error ? errorCriacao.message : 'Erro desconhecido')}`,
            );
            this.estruturaDisponivel = true;
            this.estruturaInicializada = true;
            return;
          }
          throw errorCriacao;
        }

        this.estruturaDisponivel = await this.verificarEstruturaPneusExiste();
        this.estruturaInicializada = true;
      } catch (error) {
        this.logger.error(
          `Falha ao garantir estrutura de pneus. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
        );
        this.estruturaDisponivel = false;
        this.estruturaInicializada = true;
      } finally {
        this.inicializacaoEmAndamento = null;
      }
    })();

    await this.inicializacaoEmAndamento;
  }

  private async verificarEstruturaPneusExiste(): Promise<boolean> {
    const rows = (await this.dataSource.query(
      `
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'app'
        AND table_name IN ('pneus', 'pneu_vinculos_veiculo', 'pneu_movimentacoes')
      `,
    )) as RegistroBanco[];

    const existentes = new Set(
      rows
        .map((row) => this.toText(row.table_name))
        .filter((table): table is string => Boolean(table))
        .map((table) => table.toLowerCase()),
    );

    return (
      existentes.has('pneus') &&
      existentes.has('pneu_vinculos_veiculo') &&
      existentes.has('pneu_movimentacoes')
    );
  }

  private async executarScriptCriacaoEstruturaPneus() {
    await this.dataSource.query(`
      CREATE TABLE IF NOT EXISTS app.pneus (
        id_pneu BIGSERIAL PRIMARY KEY,
        id_empresa BIGINT NOT NULL,
        numero_fogo VARCHAR(60) NOT NULL,
        marca VARCHAR(80),
        modelo VARCHAR(80),
        medida VARCHAR(40),
        tipo VARCHAR(40),
        valor NUMERIC(14,2) NOT NULL DEFAULT 0,
        status_local VARCHAR(20) NOT NULL DEFAULT 'ESTOQUE',
        ativo BOOLEAN NOT NULL DEFAULT TRUE,
        observacoes TEXT,
        usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
        criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_pneus_status_local
          CHECK (status_local IN ('ESTOQUE', 'EM_USO', 'CONSERTO', 'BAIXA', 'DESCARTE'))
      )
    `);

    await this.dataSource.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS ux_pneus_empresa_numero_fogo
      ON app.pneus (id_empresa, numero_fogo)
    `);

    await this.dataSource.query(`
      CREATE INDEX IF NOT EXISTS ix_pneus_empresa_status_local
      ON app.pneus (id_empresa, status_local)
    `);

    await this.dataSource.query(`
      CREATE TABLE IF NOT EXISTS app.pneu_vinculos_veiculo (
        id_vinculo BIGSERIAL PRIMARY KEY,
        id_empresa BIGINT NOT NULL,
        id_pneu BIGINT NOT NULL,
        id_veiculo BIGINT NOT NULL,
        posicao VARCHAR(30) NOT NULL,
        ativo BOOLEAN NOT NULL DEFAULT TRUE,
        usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
        criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    await this.dataSource.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS ux_pneu_vinculo_ativo_por_pneu
      ON app.pneu_vinculos_veiculo (id_empresa, id_pneu)
      WHERE ativo = true
    `);

    await this.dataSource.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS ux_pneu_vinculo_ativo_por_posicao
      ON app.pneu_vinculos_veiculo (id_empresa, id_veiculo, posicao)
      WHERE ativo = true
    `);

    await this.dataSource.query(`
      CREATE INDEX IF NOT EXISTS ix_pneu_vinculos_veiculo_empresa_veiculo
      ON app.pneu_vinculos_veiculo (id_empresa, id_veiculo)
    `);

    await this.dataSource.query(`
      CREATE TABLE IF NOT EXISTS app.pneu_movimentacoes (
        id_movimentacao BIGSERIAL PRIMARY KEY,
        id_empresa BIGINT NOT NULL,
        id_pneu BIGINT NOT NULL,
        id_veiculo_origem BIGINT,
        posicao_origem VARCHAR(30),
        destino VARCHAR(20) NOT NULL,
        id_veiculo_destino BIGINT,
        posicao_destino VARCHAR(30),
        motivo VARCHAR(120),
        observacoes TEXT,
        usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
        data_movimentacao TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_pneu_movimentacoes_destino
          CHECK (destino IN ('ESTOQUE', 'CONSERTO', 'BAIXA', 'DESCARTE', 'VEICULO'))
      )
    `);

    await this.dataSource.query(`
      CREATE INDEX IF NOT EXISTS ix_pneu_movimentacoes_empresa_data
      ON app.pneu_movimentacoes (id_empresa, data_movimentacao DESC)
    `);

    await this.dataSource.query(`
      CREATE INDEX IF NOT EXISTS ix_pneu_movimentacoes_empresa_pneu
      ON app.pneu_movimentacoes (id_empresa, id_pneu)
    `);
  }

  private exigirEstruturaDisponivel() {
    if (this.estruturaDisponivel) return;
    throw new BadRequestException(
      'Estrutura de pneus indisponivel no banco. Execute o script sql/pneus_schema.sql.',
    );
  }

  private tratarErroPersistencia(error: unknown, acao: string): never {
    if (error instanceof BadRequestException || error instanceof NotFoundException) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as { code?: string; message?: string };
      this.logger.error(
        `Falha ao ${acao} pneu. code=${erroPg.code ?? 'N/A'} message=${erroPg.message ?? 'Erro desconhecido'}`,
      );

      if (erroPg.code === '23505') {
        throw new BadRequestException(
          'Ja existe um pneu com o mesmo numero de fogo para esta empresa.',
        );
      }
      if (erroPg.code === '23503') {
        throw new BadRequestException('Veiculo informado nao existe para a empresa logada.');
      }
      if (erroPg.code === '23514') {
        throw new BadRequestException('Dados invalidos para as regras de cadastro de pneus.');
      }
      if (erroPg.code === '42501') {
        throw new BadRequestException('Usuario do banco sem permissao para gravar em app.pneus.');
      }
      if (erroPg.code === '42P01') {
        throw new BadRequestException(
          'Tabela de pneus nao encontrada. Execute o script sql/pneus_schema.sql.',
        );
      }
    }

    this.logger.error(
      `Falha ao ${acao} pneu sem codigo SQL mapeado. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
    );
    throw new BadRequestException(`Nao foi possivel ${acao} o pneu neste momento.`);
  }
}
