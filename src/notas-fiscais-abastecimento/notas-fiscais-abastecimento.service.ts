import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import {
  CriarNotaFiscalAbastecimentoDto,
  CriarNotaFiscalAbastecimentoItemDto,
} from './dto/criar-nota-fiscal-abastecimento.dto';

type RegistroBanco = Record<string, unknown>;

type NotaFiscalAbastecimentoItem = {
  idItemNotaFiscal: number;
  idNotaFiscal: number;
  idEmpresa: number;
  idAbastecimento: number | null;
  idProduto: number | null;
  numeroItem: number;
  codigoProduto: string | null;
  codigoProdutoXml: string | null;
  descricaoProduto: string;
  codigoAnp: string | null;
  ncm: string | null;
  cfop: string | null;
  unidade: string | null;
  quantidade: number;
  litros: number;
  valorUnitario: number;
  valorDesconto: number;
  valorTotal: number;
  combustivel: string | null;
  observacoes: string | null;
  usuarioAtualizacao: string | null;
  criadoEm: Date | null;
  atualizadoEm: Date | null;
};

type NotaFiscalAbastecimento = {
  idNotaFiscal: number;
  idEmpresa: number;
  idFornecedor: number | null;
  idVeiculo: number | null;
  idMotorista: number | null;
  idViagem: number | null;
  chaveAcesso: string | null;
  numeroNf: string | null;
  serieNf: string | null;
  modeloNf: string | null;
  origemLancamento: string;
  statusDocumento: string;
  statusFiscal: string;
  statusOperacional: string;
  dataEmissao: Date;
  dataAbastecimento: Date | null;
  dataEntrada: Date | null;
  protocoloAutorizacao: string | null;
  cnpjEmitente: string | null;
  razaoSocialEmitente: string;
  nomeFantasiaEmitente: string | null;
  ufEmitente: string | null;
  placaVeiculoInformada: string | null;
  kmInformado: number | null;
  totalLitros: number;
  valorTotalProdutos: number;
  valorTotalDesconto: number;
  valorTotalNota: number;
  valorTotalIcms: number;
  observacoes: string | null;
  motivoStatus: string | null;
  efetivadaEm: Date | null;
  efetivadaPor: string | null;
  xmlOriginal: string | null;
  xmlExtraidoJson: Record<string, unknown>;
  usuarioAtualizacao: string | null;
  criadoEm: Date | null;
  atualizadoEm: Date | null;
  itens: NotaFiscalAbastecimentoItem[];
};

type ItemNotaFiscalNormalizado = {
  idProduto: number | null;
  numeroItem: number;
  codigoProduto: string | null;
  codigoProdutoXml: string | null;
  descricaoProduto: string;
  codigoAnp: string | null;
  ncm: string | null;
  cfop: string | null;
  unidade: string | null;
  quantidade: number;
  litros: number;
  valorUnitario: number;
  valorDesconto: number;
  valorTotal: number;
  combustivel: string | null;
  observacoes: string | null;
};

type PayloadNotaFiscalNormalizado = {
  idViagem: number | null;
  idFornecedor: number | null;
  idVeiculo: number | null;
  idMotorista: number | null;
  chaveAcesso: string | null;
  numeroNf: string | null;
  serieNf: string | null;
  modeloNf: string | null;
  origemLancamento: string;
  statusDocumento: string;
  statusFiscal: string;
  statusOperacional: string;
  dataEmissao: Date;
  dataAbastecimento: Date | null;
  protocoloAutorizacao: string | null;
  cnpjEmitente: string | null;
  razaoSocialEmitente: string;
  nomeFantasiaEmitente: string | null;
  ufEmitente: string | null;
  placaVeiculoInformada: string | null;
  kmInformado: number | null;
  totalLitros: number;
  valorTotalProdutos: number;
  valorTotalDesconto: number;
  valorTotalNota: number;
  valorTotalIcms: number;
  observacoes: string | null;
  motivoStatus: string | null;
  efetivadaEm: Date | null;
  efetivadaPor: string | null;
  xmlOriginal: string | null;
  xmlExtraidoJson: Record<string, unknown>;
  usuarioAtualizacao: string;
  itens: ItemNotaFiscalNormalizado[];
};

type ColunasAbastecimentoNota = {
  idEmpresa: string | null;
  idVeiculo: string;
  idFornecedor: string;
  idViagem: string | null;
  dataAbastecimento: string;
  litros: string;
  valorLitro: string;
  valorTotal: string | null;
  km: string;
  observacao: string | null;
  usuarioAtualizacao: string | null;
  origemLancamento: string | null;
  idNotaFiscal: string | null;
};

type NotaImportada = {
  chaveAcesso: string | null;
  numeroNf: string | null;
  serieNf: string | null;
  modeloNf: string | null;
  dataEmissao: string;
  dataAbastecimento: string | null;
  protocoloAutorizacao: string | null;
  cnpjEmitente: string | null;
  razaoSocialEmitente: string;
  nomeFantasiaEmitente: string | null;
  ufEmitente: string | null;
  placaVeiculoInformada: string | null;
  kmInformado: number | null;
  totalLitros: number;
  valorTotalProdutos: number;
  valorTotalDesconto: number;
  valorTotalNota: number;
  valorTotalIcms: number;
  observacoes: string | null;
  itens: Array<{
    idProduto?: number | null;
    numeroItem: number;
    codigoProduto: string | null;
    codigoProdutoXml?: string | null;
    descricaoProduto: string;
    codigoAnp: string | null;
    ncm: string | null;
    cfop: string | null;
    unidade: string | null;
    quantidade: number;
    litros: number;
    valorUnitario: number;
    valorDesconto: number;
    valorTotal: number;
    combustivel: string | null;
  }>;
  xmlExtraidoJson: Record<string, unknown>;
};

@Injectable()
export class NotasFiscaisAbastecimentoService {
  constructor(private readonly dataSource: DataSource) {}

  async listarTodos(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const notasRows = (await manager.query(
        `
          SELECT *
          FROM app.notas_fiscais_abastecimento
          WHERE id_empresa = $1
          ORDER BY data_emissao DESC, id_nota_fiscal DESC
        `,
        [String(idEmpresa)],
      )) as RegistroBanco[];

      const itensRows = (await manager.query(
        `
          SELECT *
          FROM app.notas_fiscais_abastecimento_itens
          WHERE id_empresa = $1
          ORDER BY id_nota_fiscal DESC, numero_item ASC
        `,
        [String(idEmpresa)],
      )) as RegistroBanco[];

      const itensPorNota = new Map<number, NotaFiscalAbastecimentoItem[]>();
      for (const itemRow of itensRows) {
        const item = this.mapearItem(itemRow);
        const lista = itensPorNota.get(item.idNotaFiscal) ?? [];
        lista.push(item);
        itensPorNota.set(item.idNotaFiscal, lista);
      }

      const notas = notasRows.map((row) =>
        this.mapearNota(
          row,
          itensPorNota.get(this.toNumber(row.id_nota_fiscal) ?? 0) ?? [],
        ),
      );

      return {
        sucesso: true,
        total: notas.length,
        notasFiscais: notas,
      };
    });
  }

  async buscarPorId(idEmpresa: number, idNotaFiscal: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const notaRow = await this.buscarNotaRowOuFalhar(
        manager,
        idEmpresa,
        idNotaFiscal,
      );

      const itensRows = (await manager.query(
        `
          SELECT *
          FROM app.notas_fiscais_abastecimento_itens
          WHERE id_empresa = $1
            AND id_nota_fiscal = $2
          ORDER BY numero_item ASC
        `,
        [String(idEmpresa), idNotaFiscal],
      )) as RegistroBanco[];

      return {
        sucesso: true,
        notaFiscal: this.mapearNota(
          notaRow,
          itensRows.map((row) => this.mapearItem(row)),
        ),
      };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarNotaFiscalAbastecimentoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const payload = this.normalizarPayloadCriacao(dados, usuarioJwt);

    return this.executarComRls(idEmpresa, async (manager) => {
      try {
        const notaRows = (await manager.query(
          `
            INSERT INTO app.notas_fiscais_abastecimento (
              id_empresa,
              id_fornecedor,
              id_veiculo,
              id_motorista,
              id_viagem,
              chave_acesso,
              numero_nf,
              serie_nf,
              modelo_nf,
              origem_lancamento,
              status_documento,
              data_emissao,
              data_abastecimento,
              protocolo_autorizacao,
              cnpj_emitente,
              razao_social_emitente,
              nome_fantasia_emitente,
              uf_emitente,
              placa_veiculo_informada,
              km_informado,
              total_litros,
              valor_total_produtos,
              valor_total_desconto,
              valor_total_nota,
              valor_total_icms,
              observacoes,
              status_fiscal,
              status_operacional,
              motivo_status,
              efetivada_em,
              efetivada_por,
              xml_original,
              xml_extraido_json,
              usuario_atualizacao
            )
            VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
              $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
              $21,$22,$23,$24,$25,$26,$27,$28::jsonb,$29,
              $30,$31,$32,$33,$34
            )
            RETURNING *
          `,
          [
            String(idEmpresa),
            payload.idFornecedor,
            payload.idVeiculo,
            payload.idMotorista,
            payload.idViagem,
            payload.chaveAcesso,
            payload.numeroNf,
            payload.serieNf,
            payload.modeloNf,
            payload.origemLancamento,
            payload.statusDocumento,
            payload.dataEmissao,
            payload.dataAbastecimento,
            payload.protocoloAutorizacao,
            payload.cnpjEmitente,
            payload.razaoSocialEmitente,
            payload.nomeFantasiaEmitente,
            payload.ufEmitente,
            payload.placaVeiculoInformada,
            payload.kmInformado,
            payload.totalLitros,
            payload.valorTotalProdutos,
            payload.valorTotalDesconto,
            payload.valorTotalNota,
            payload.valorTotalIcms,
            payload.observacoes,
            payload.xmlOriginal,
            JSON.stringify(payload.xmlExtraidoJson ?? {}),
            payload.usuarioAtualizacao,
            payload.statusFiscal,
            payload.statusOperacional,
            payload.motivoStatus,
            payload.efetivadaEm,
            payload.efetivadaPor,
          ],
        )) as RegistroBanco[];

        const notaRow = notaRows[0];
        const idNotaFiscal = this.toNumber(notaRow?.id_nota_fiscal);
        if (!notaRow || !idNotaFiscal) {
          throw new BadRequestException('Falha ao cadastrar nota fiscal.');
        }

        await this.inserirItensDaNota(manager, idEmpresa, idNotaFiscal, payload);

        await this.gerarAbastecimentoDaNota(
          manager,
          idEmpresa,
          idNotaFiscal,
          payload,
        );

        const itensRows = (await manager.query(
          `
            SELECT *
            FROM app.notas_fiscais_abastecimento_itens
            WHERE id_empresa = $1
              AND id_nota_fiscal = $2
            ORDER BY numero_item ASC
          `,
          [String(idEmpresa), idNotaFiscal],
        )) as RegistroBanco[];

        return {
          sucesso: true,
          mensagem: 'Nota fiscal de abastecimento cadastrada com sucesso.',
          notaFiscal: this.mapearNota(
            notaRow,
            itensRows.map((row) => this.mapearItem(row)),
          ),
        };
      } catch (error) {
        this.tratarErroPersistencia(error, 'cadastrar nota fiscal');
      }
    });
  }

  async atualizar(
    idEmpresa: number,
    idNotaFiscal: number,
    dados: CriarNotaFiscalAbastecimentoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const payload = this.normalizarPayloadCriacao(dados, usuarioJwt);

    return this.executarComRls(idEmpresa, async (manager) => {
      await this.buscarNotaRowOuFalhar(manager, idEmpresa, idNotaFiscal);

      try {
        const notaRows = (await manager.query(
          `
            UPDATE app.notas_fiscais_abastecimento
            SET
              id_fornecedor = $3,
              id_veiculo = $4,
              id_motorista = $5,
              id_viagem = $6,
              chave_acesso = $7,
              numero_nf = $8,
              serie_nf = $9,
              modelo_nf = $10,
              origem_lancamento = $11,
              status_documento = $12,
              data_emissao = $13,
              data_abastecimento = $14,
              protocolo_autorizacao = $15,
              cnpj_emitente = $16,
              razao_social_emitente = $17,
              nome_fantasia_emitente = $18,
              uf_emitente = $19,
              placa_veiculo_informada = $20,
              km_informado = $21,
              total_litros = $22,
              valor_total_produtos = $23,
              valor_total_desconto = $24,
              valor_total_nota = $25,
              valor_total_icms = $26,
              observacoes = $27,
              status_fiscal = $28,
              status_operacional = $29,
              motivo_status = $30,
              efetivada_em = $31,
              efetivada_por = $32,
              xml_original = $33,
              xml_extraido_json = $34::jsonb,
              usuario_atualizacao = $35,
              atualizado_em = NOW()
            WHERE id_empresa = $1
              AND id_nota_fiscal = $2
            RETURNING *
          `,
          [
            String(idEmpresa),
            idNotaFiscal,
            payload.idFornecedor,
            payload.idVeiculo,
            payload.idMotorista,
            payload.idViagem,
            payload.chaveAcesso,
            payload.numeroNf,
            payload.serieNf,
            payload.modeloNf,
            payload.origemLancamento,
            payload.statusDocumento,
            payload.dataEmissao,
            payload.dataAbastecimento,
            payload.protocoloAutorizacao,
            payload.cnpjEmitente,
            payload.razaoSocialEmitente,
            payload.nomeFantasiaEmitente,
            payload.ufEmitente,
            payload.placaVeiculoInformada,
            payload.kmInformado,
            payload.totalLitros,
            payload.valorTotalProdutos,
            payload.valorTotalDesconto,
            payload.valorTotalNota,
            payload.valorTotalIcms,
            payload.observacoes,
            payload.statusFiscal,
            payload.statusOperacional,
            payload.motivoStatus,
            payload.efetivadaEm,
            payload.efetivadaPor,
            payload.xmlOriginal,
            JSON.stringify(payload.xmlExtraidoJson ?? {}),
            payload.usuarioAtualizacao,
          ],
        )) as RegistroBanco[];

        await manager.query(
          `
            DELETE FROM app.abastecimentos
            WHERE id_empresa = $1
              AND id_nota_fiscal = $2
              AND (
                origem_lancamento = 'NOTA_FISCAL'
                OR origem_lancamento IS NULL
              )
          `,
          [String(idEmpresa), idNotaFiscal],
        );

        await manager.query(
          `
            DELETE FROM app.notas_fiscais_abastecimento_itens
            WHERE id_empresa = $1
              AND id_nota_fiscal = $2
          `,
          [String(idEmpresa), idNotaFiscal],
        );

        await this.inserirItensDaNota(manager, idEmpresa, idNotaFiscal, payload);
        await this.gerarAbastecimentoDaNota(manager, idEmpresa, idNotaFiscal, payload);

        const itensRows = (await manager.query(
          `
            SELECT *
            FROM app.notas_fiscais_abastecimento_itens
            WHERE id_empresa = $1
              AND id_nota_fiscal = $2
            ORDER BY numero_item ASC
          `,
          [String(idEmpresa), idNotaFiscal],
        )) as RegistroBanco[];

        return {
          sucesso: true,
          mensagem: 'Nota fiscal de abastecimento atualizada com sucesso.',
          notaFiscal: this.mapearNota(
            notaRows[0],
            itensRows.map((row) => this.mapearItem(row)),
          ),
        };
      } catch (error) {
        this.tratarErroPersistencia(error, 'atualizar nota fiscal');
      }
    });
  }

  async importarXml(idEmpresa: number, xml: string) {
    const xmlLimpo = this.normalizarXml(xml);
    if (!xmlLimpo.includes('<')) {
      throw new BadRequestException('Conteudo XML invalido.');
    }

    const nota = this.extrairNotaDoXml(xmlLimpo);
    return this.executarComRls(idEmpresa, async (manager) => {
      const sugestoes = await this.carregarSugestoesPorXml(manager, idEmpresa, nota);
      return {
        sucesso: true,
        mensagem:
          'XML processado com sucesso. Revise e complemente os dados antes de salvar.',
        notaFiscal: {
          ...nota,
          origemLancamento: 'XML',
          statusDocumento: 'PENDENTE',
          statusFiscal: 'AUTORIZADA',
          statusOperacional:
            !sugestoes.idFornecedor ||
            !sugestoes.idVeiculo ||
            !nota.kmInformado ||
            sugestoes.itens.some((item) => !item.idProduto)
              ? 'AGUARDANDO_VINCULOS'
              : 'PRONTA',
          idFornecedor: sugestoes.idFornecedor,
          idVeiculo: sugestoes.idVeiculo,
          idMotorista: sugestoes.idMotorista,
          idViagem: sugestoes.idViagem,
          itens: nota.itens.map((item) => {
            const sugestaoProduto = sugestoes.itens.find(
              (sugestao) => sugestao.numeroItem === item.numeroItem,
            );

            return {
              ...item,
              idProduto: sugestaoProduto?.idProduto ?? null,
              codigoProduto: sugestaoProduto?.codigoProdutoSistema ?? null,
              codigoProdutoXml: sugestaoProduto?.codigoProdutoXml ?? item.codigoProduto ?? null,
            };
          }),
        },
        sugestoes,
      };
    });
  }

  async remover(idEmpresa: number, idNotaFiscal: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      await this.buscarNotaRowOuFalhar(manager, idEmpresa, idNotaFiscal);

      await manager.query(
        `
          DELETE FROM app.abastecimentos
          WHERE id_empresa = $1
            AND id_nota_fiscal = $2
            AND (
              origem_lancamento = 'NOTA_FISCAL'
              OR origem_lancamento IS NULL
            )
        `,
        [String(idEmpresa), idNotaFiscal],
      );

      await manager.query(
        `
          DELETE FROM app.notas_fiscais_abastecimento
          WHERE id_empresa = $1
            AND id_nota_fiscal = $2
        `,
        [String(idEmpresa), idNotaFiscal],
      );

      return {
        sucesso: true,
        mensagem:
          'Nota fiscal excluida com sucesso. Os abastecimentos gerados por ela tambem foram removidos.',
        idNotaFiscal,
      };
    });
  }

  async efetivar(
    idEmpresa: number,
    idNotaFiscal: number,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const notaRow = await this.buscarNotaRowOuFalhar(manager, idEmpresa, idNotaFiscal);
      const itensRows = (await manager.query(
        `
          SELECT *
          FROM app.notas_fiscais_abastecimento_itens
          WHERE id_empresa = $1
            AND id_nota_fiscal = $2
          ORDER BY numero_item ASC
        `,
        [String(idEmpresa), idNotaFiscal],
      )) as RegistroBanco[];

      const notaAtual = this.mapearNota(
        notaRow,
        itensRows.map((row) => this.mapearItem(row)),
      );

      const payload = this.normalizarPayloadDaNotaExistente(notaAtual, usuarioJwt);
      const statusOperacional = this.resolverStatusOperacional(payload);

      if (payload.statusFiscal !== 'AUTORIZADA') {
        throw new BadRequestException(
          'A nota fiscal so pode ser efetivada quando a situacao fiscal for AUTORIZADA.',
        );
      }

      if (statusOperacional !== 'PRONTA' && statusOperacional !== 'LANCADA') {
        throw new BadRequestException(
          'A nota fiscal ainda nao esta pronta para efetivacao. Revise os vinculos obrigatorios.',
        );
      }

      await manager.query(
        `
          DELETE FROM app.abastecimentos
          WHERE id_empresa = $1
            AND id_nota_fiscal = $2
            AND (
              origem_lancamento = 'NOTA_FISCAL'
              OR origem_lancamento IS NULL
            )
        `,
        [String(idEmpresa), idNotaFiscal],
      );

      await this.gerarAbastecimentoDaNota(manager, idEmpresa, idNotaFiscal, {
        ...payload,
        statusOperacional: 'LANCADA',
        efetivadaEm: new Date(),
        efetivadaPor: this.normalizarUsuario(usuarioJwt.email),
      });

      const notaRowsAtualizadas = (await manager.query(
        `
          UPDATE app.notas_fiscais_abastecimento
          SET
            status_documento = 'PROCESSADA',
            status_fiscal = 'AUTORIZADA',
            status_operacional = 'LANCADA',
            efetivada_em = NOW(),
            efetivada_por = $3,
            usuario_atualizacao = $3,
            atualizado_em = NOW()
          WHERE id_empresa = $1
            AND id_nota_fiscal = $2
          RETURNING *
        `,
        [String(idEmpresa), idNotaFiscal, this.normalizarUsuario(usuarioJwt.email)],
      )) as RegistroBanco[];

      return {
        sucesso: true,
        mensagem: 'Nota fiscal efetivada com sucesso.',
        notaFiscal: this.mapearNota(
          notaRowsAtualizadas[0],
          itensRows.map((row) => this.mapearItem(row)),
        ),
      };
    });
  }

  private async carregarSugestoesPorXml(
    manager: EntityManager,
    idEmpresa: number,
    nota: NotaImportada,
  ) {
    let idFornecedor: number | null = null;
    let idVeiculo: number | null = null;
    let idMotorista: number | null = null;
    let idViagem: number | null = null;

    if (nota.cnpjEmitente) {
      const fornecedorRows = (await manager.query(
        `
          SELECT id_fornecedor
          FROM app.fornecedor
          WHERE id_empresa = $1
            AND cnpj = $2
          LIMIT 1
        `,
        [String(idEmpresa), nota.cnpjEmitente],
      )) as RegistroBanco[];

      idFornecedor = this.toNumber(fornecedorRows[0]?.id_fornecedor);
    }

    if (nota.placaVeiculoInformada) {
      const veiculoRows = (await manager.query(
        `
          SELECT id_veiculo, id_motorista_atual, placa, placa2, placa3, placa4
          FROM app.veiculo
          WHERE id_empresa = $1
        `,
        [String(idEmpresa)],
      )) as RegistroBanco[];

      const placaNormalizada = this.normalizarPlaca(nota.placaVeiculoInformada);
      const veiculo = veiculoRows.find((row) => {
        const placas = [
          this.toText(row.placa),
          this.toText(row.placa2),
          this.toText(row.placa3),
          this.toText(row.placa4),
        ]
          .filter((item): item is string => Boolean(item))
          .map((item) => this.normalizarPlaca(item))
          .filter((item): item is string => Boolean(item));

        return placas.includes(placaNormalizada ?? '');
      });

      idVeiculo = this.toNumber(veiculo?.id_veiculo);
      idMotorista = this.toNumber(veiculo?.id_motorista_atual);
      idViagem = await this.buscarViagemAbertaMaisRecentePorVeiculo(
        manager,
        idEmpresa,
        idVeiculo,
      );
    }

    return {
      idFornecedor,
      idVeiculo,
      idMotorista,
      idViagem,
      placaVeiculoInformada: nota.placaVeiculoInformada,
      fornecedorEncontrado: Boolean(idFornecedor),
      precisaCadastrarFornecedor: !idFornecedor,
      itens: await this.sugerirProdutosDosItens(manager, idEmpresa, nota.itens),
    };
  }

  private async sugerirProdutosDosItens(
    manager: EntityManager,
    idEmpresa: number,
    itens: NotaImportada['itens'],
  ) {
    const produtosRows = (await manager.query(
      `
        SELECT id_produto, descricao_produto, codigo_original
        FROM app.produto
        WHERE id_empresa = $1
        ORDER BY descricao_produto ASC
      `,
      [String(idEmpresa)],
    )) as RegistroBanco[];

    return itens.map((item) => {
      const codigoXml = item.codigoProduto?.trim().toUpperCase() ?? null;
      const descricaoItem = item.descricaoProduto.trim().toUpperCase();

      const produtoPorCodigo = codigoXml
        ? produtosRows.find((row) => {
            const codigoOriginal = this.toText(row.codigo_original)?.toUpperCase() ?? null;
            return codigoOriginal !== null && codigoOriginal === codigoXml;
          })
        : null;

      const produto =
        produtoPorCodigo ??
        produtosRows.find((row) => {
          const descricaoProduto = this.toText(row.descricao_produto)?.toUpperCase() ?? '';
          return descricaoProduto === descricaoItem;
        });

      return {
        numeroItem: item.numeroItem,
        idProduto: this.toNumber(produto?.id_produto),
        codigoProdutoSistema: this.toNumber(produto?.id_produto)?.toString() ?? null,
        codigoProdutoXml: codigoXml,
        descricaoProdutoSistema: this.toText(produto?.descricao_produto),
        produtoEncontrado: Boolean(produto),
        precisaCadastrarProduto: !produto,
      };
    });
  }

  private async buscarViagemAbertaMaisRecentePorVeiculo(
    manager: EntityManager,
    idEmpresa: number,
    idVeiculo: number | null,
  ): Promise<number | null> {
    if (!idVeiculo) {
      return null;
    }

    const rows = (await manager.query(
      `
        SELECT id_viagem
        FROM app.viagens
        WHERE id_veiculo = $1
          AND id_empresa = $2
          AND status = 'A'
          AND data_fim IS NULL
        ORDER BY data_inicio DESC, id_viagem DESC
        LIMIT 1
      `,
      [idVeiculo, String(idEmpresa)],
    )) as RegistroBanco[];

    return this.toNumber(rows[0]?.id_viagem);
  }

  private async buscarNotaRowOuFalhar(
    manager: EntityManager,
    idEmpresa: number,
    idNotaFiscal: number,
  ) {
    const rows = (await manager.query(
      `
        SELECT *
        FROM app.notas_fiscais_abastecimento
        WHERE id_empresa = $1
          AND id_nota_fiscal = $2
        LIMIT 1
      `,
      [String(idEmpresa), idNotaFiscal],
    )) as RegistroBanco[];

    const nota = rows[0];
    if (!nota) {
      throw new NotFoundException(
        'Nota fiscal de abastecimento nao encontrada para a empresa logada.',
      );
    }

    return nota;
  }

  private normalizarPayloadCriacao(
    dados: CriarNotaFiscalAbastecimentoDto,
    usuarioJwt: JwtUsuarioPayload,
  ): PayloadNotaFiscalNormalizado {
    const itens = dados.itens.map((item) => this.normalizarItem(item));
    const totalLitrosItens = itens.reduce((acc, item) => acc + item.litros, 0);
    const totalProdutosItens = itens.reduce(
      (acc, item) => acc + item.valorTotal,
      0,
    );
    const totalDescontoItens = itens.reduce(
      (acc, item) => acc + item.valorDesconto,
      0,
    );

    const statusFiscal = this.resolverStatusFiscal(dados);
    const payloadBase: Omit<PayloadNotaFiscalNormalizado, 'statusOperacional'> = {
      idViagem: dados.idViagem ?? null,
      idFornecedor: dados.idFornecedor ?? null,
      idVeiculo: dados.idVeiculo ?? null,
      idMotorista: dados.idMotorista ?? null,
      chaveAcesso: this.extrairSomenteDigitos(dados.chaveAcesso ?? null, 44),
      numeroNf: this.nullIfBlank(dados.numeroNf),
      serieNf: this.nullIfBlank(dados.serieNf),
      modeloNf: this.nullIfBlank(dados.modeloNf),
      origemLancamento:
        this.nullIfBlank(dados.origemLancamento)?.toUpperCase() ?? 'MANUAL',
      statusDocumento: this.mapearStatusDocumentoPorStatusFiscal(statusFiscal),
      statusFiscal,
      dataEmissao: new Date(dados.dataEmissao),
      dataAbastecimento: dados.dataAbastecimento
        ? new Date(dados.dataAbastecimento)
        : null,
      protocoloAutorizacao: this.nullIfBlank(dados.protocoloAutorizacao),
      cnpjEmitente: this.extrairSomenteDigitos(dados.cnpjEmitente ?? null, 14),
      razaoSocialEmitente:
        this.normalizarTextoObrigatorio(dados.razaoSocialEmitente),
      nomeFantasiaEmitente: this.nullIfBlank(dados.nomeFantasiaEmitente),
      ufEmitente: this.nullIfBlank(dados.ufEmitente)?.toUpperCase() ?? null,
      placaVeiculoInformada: this.nullIfBlank(dados.placaVeiculoInformada)
        ? this.normalizarPlaca(dados.placaVeiculoInformada ?? '')
        : null,
      kmInformado:
        dados.kmInformado !== undefined && dados.kmInformado !== null
          ? dados.kmInformado
          : null,
      totalLitros: this.fix3(dados.totalLitros ?? totalLitrosItens),
      valorTotalProdutos: this.fix2(
        dados.valorTotalProdutos ?? totalProdutosItens,
      ),
      valorTotalDesconto: this.fix2(
        dados.valorTotalDesconto ?? totalDescontoItens,
      ),
      valorTotalNota: this.fix2(dados.valorTotalNota ?? totalProdutosItens),
      valorTotalIcms: this.fix2(dados.valorTotalIcms ?? 0),
      observacoes: this.nullIfBlank(dados.observacoes),
      xmlOriginal: this.nullIfBlank(dados.xmlOriginal ?? null),
      xmlExtraidoJson: dados.xmlExtraidoJson ?? {},
      usuarioAtualizacao: this.normalizarUsuario(
        dados.usuarioAtualizacao ?? usuarioJwt.email,
      ),
      itens,
      motivoStatus: this.nullIfBlank(dados.motivoStatus),
      efetivadaEm: null as Date | null,
      efetivadaPor: null as string | null,
    };

    return {
      ...payloadBase,
      statusOperacional: this.resolverStatusOperacional({
        ...payloadBase,
        statusOperacional: this.nullIfBlank(dados.statusOperacional)?.toUpperCase() ?? null,
      }),
    };
  }

  private normalizarPayloadDaNotaExistente(
    nota: NotaFiscalAbastecimento,
    usuarioJwt: JwtUsuarioPayload,
  ): PayloadNotaFiscalNormalizado {
    const itens = nota.itens.map((item) => this.normalizarItem(item));
    const payloadBase: Omit<PayloadNotaFiscalNormalizado, 'statusOperacional'> = {
      idViagem: nota.idViagem ?? null,
      idFornecedor: nota.idFornecedor ?? null,
      idVeiculo: nota.idVeiculo ?? null,
      idMotorista: nota.idMotorista ?? null,
      chaveAcesso: this.extrairSomenteDigitos(nota.chaveAcesso ?? null, 44),
      numeroNf: this.nullIfBlank(nota.numeroNf),
      serieNf: this.nullIfBlank(nota.serieNf),
      modeloNf: this.nullIfBlank(nota.modeloNf),
      origemLancamento: this.nullIfBlank(nota.origemLancamento)?.toUpperCase() ?? 'MANUAL',
      statusDocumento: this.mapearStatusDocumentoPorStatusFiscal(
        this.nullIfBlank(nota.statusFiscal)?.toUpperCase() ?? 'PENDENTE',
      ),
      statusFiscal: this.nullIfBlank(nota.statusFiscal)?.toUpperCase() ?? 'PENDENTE',
      dataEmissao: nota.dataEmissao,
      dataAbastecimento: nota.dataAbastecimento,
      protocoloAutorizacao: this.nullIfBlank(nota.protocoloAutorizacao),
      cnpjEmitente: this.extrairSomenteDigitos(nota.cnpjEmitente ?? null, 14),
      razaoSocialEmitente: this.normalizarTextoObrigatorio(nota.razaoSocialEmitente),
      nomeFantasiaEmitente: this.nullIfBlank(nota.nomeFantasiaEmitente),
      ufEmitente: this.nullIfBlank(nota.ufEmitente)?.toUpperCase() ?? null,
      placaVeiculoInformada: this.nullIfBlank(nota.placaVeiculoInformada)
        ? this.normalizarPlaca(nota.placaVeiculoInformada ?? '')
        : null,
      kmInformado: nota.kmInformado,
      totalLitros: this.fix3(nota.totalLitros),
      valorTotalProdutos: this.fix2(nota.valorTotalProdutos),
      valorTotalDesconto: this.fix2(nota.valorTotalDesconto),
      valorTotalNota: this.fix2(nota.valorTotalNota),
      valorTotalIcms: this.fix2(nota.valorTotalIcms),
      observacoes: this.nullIfBlank(nota.observacoes),
      xmlOriginal: this.nullIfBlank(nota.xmlOriginal ?? null),
      xmlExtraidoJson: nota.xmlExtraidoJson ?? {},
      usuarioAtualizacao: this.normalizarUsuario(usuarioJwt.email),
      itens,
      motivoStatus: this.nullIfBlank(nota.motivoStatus),
      efetivadaEm: nota.efetivadaEm,
      efetivadaPor: this.nullIfBlank(nota.efetivadaPor),
    };

    return {
      ...payloadBase,
      statusOperacional: this.resolverStatusOperacional(payloadBase),
    };
  }

  private resolverStatusFiscal(
    dados: Pick<
      CriarNotaFiscalAbastecimentoDto,
      'statusFiscal' | 'statusDocumento'
    >,
  ): string {
    const statusFiscalInformado = this.nullIfBlank(dados.statusFiscal)?.toUpperCase();
    if (statusFiscalInformado) {
      return statusFiscalInformado;
    }

    const statusDocumento = this.nullIfBlank(dados.statusDocumento)?.toUpperCase();
    switch (statusDocumento) {
      case 'PROCESSADA':
        return 'AUTORIZADA';
      case 'CANCELADA':
        return 'CANCELADA';
      case 'ERRO':
        return 'ERRO_IMPORTACAO';
      default:
        return 'PENDENTE';
    }
  }

  private mapearStatusDocumentoPorStatusFiscal(statusFiscal: string): string {
    switch (statusFiscal) {
      case 'AUTORIZADA':
        return 'PROCESSADA';
      case 'CANCELADA':
        return 'CANCELADA';
      case 'ERRO_IMPORTACAO':
        return 'ERRO';
      default:
        return 'PENDENTE';
    }
  }

  private resolverStatusOperacional(
    payload: {
      statusFiscal: string;
      statusOperacional?: string | null;
      idFornecedor: number | null;
      idVeiculo: number | null;
      kmInformado: number | null;
      itens: ItemNotaFiscalNormalizado[];
    },
  ): string {
    const statusFiscal = payload.statusFiscal?.toUpperCase() ?? 'PENDENTE';
    if (statusFiscal === 'CANCELADA') {
      return 'BLOQUEADA';
    }
    if (statusFiscal === 'ERRO_IMPORTACAO') {
      return 'DIVERGENCIA';
    }

    const statusInformado = this.nullIfBlank(payload.statusOperacional)?.toUpperCase();
    if (statusInformado === 'BLOQUEADA' || statusInformado === 'DIVERGENCIA') {
      return statusInformado;
    }

    const faltaVinculoCabecalho =
      !payload.idFornecedor || !payload.idVeiculo || payload.kmInformado === null;
    const faltaVinculoProduto = payload.itens.some((item) => !item.idProduto);

    if (faltaVinculoCabecalho || faltaVinculoProduto) {
      return 'AGUARDANDO_VINCULOS';
    }

    if (statusInformado === 'LANCADA') {
      return 'LANCADA';
    }

    return 'PRONTA';
  }

  private async inserirItensDaNota(
    manager: EntityManager,
    idEmpresa: number,
    idNotaFiscal: number,
    payload: PayloadNotaFiscalNormalizado,
  ): Promise<void> {
    for (const item of payload.itens) {
      await manager.query(
        `
          INSERT INTO app.notas_fiscais_abastecimento_itens (
            id_nota_fiscal,
            id_empresa,
            id_abastecimento,
            id_produto,
            numero_item,
            codigo_produto,
            codigo_produto_xml,
            descricao_produto,
            codigo_anp,
            ncm,
            cfop,
            unidade,
            quantidade,
            litros,
            valor_unitario,
            valor_desconto,
            valor_total,
            combustivel,
            observacoes,
            usuario_atualizacao
          )
          VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
            $11,$12,$13,$14,$15,$16,$17,$18,$19,$20
          )
        `,
        [
          idNotaFiscal,
          String(idEmpresa),
          null,
          item.idProduto,
          item.numeroItem,
          item.codigoProduto,
          item.codigoProdutoXml,
          item.descricaoProduto,
          item.codigoAnp,
          item.ncm,
          item.cfop,
          item.unidade,
          item.quantidade,
          item.litros,
          item.valorUnitario,
          item.valorDesconto,
          item.valorTotal,
          item.combustivel,
          item.observacoes,
          payload.usuarioAtualizacao,
        ],
      );
    }
  }

  private normalizarItem(
    item: CriarNotaFiscalAbastecimentoItemDto | NotaFiscalAbastecimentoItem,
  ): ItemNotaFiscalNormalizado {
    const quantidade = this.fix3(item.quantidade ?? item.litros ?? 0);
    const litros = this.fix3(item.litros ?? item.quantidade ?? 0);
    const valorUnitario = this.fix6(item.valorUnitario ?? 0);
    const valorDesconto = this.fix2(item.valorDesconto ?? 0);
    const valorTotal = this.fix2(
      item.valorTotal ?? Number((quantidade * valorUnitario).toFixed(2)),
    );

    return {
      idProduto: item.idProduto ?? null,
      numeroItem: item.numeroItem,
      codigoProduto: item.idProduto ? String(item.idProduto) : this.nullIfBlank(item.codigoProduto),
      codigoProdutoXml: this.nullIfBlank(item.codigoProdutoXml ?? item.codigoProduto),
      descricaoProduto: this.normalizarTextoObrigatorio(item.descricaoProduto),
      codigoAnp: this.nullIfBlank(item.codigoAnp),
      ncm: this.nullIfBlank(item.ncm),
      cfop: this.nullIfBlank(item.cfop),
      unidade: this.nullIfBlank(item.unidade),
      quantidade,
      litros,
      valorUnitario,
      valorDesconto,
      valorTotal,
      combustivel: this.nullIfBlank(item.combustivel),
      observacoes: this.nullIfBlank(item.observacoes),
    };
  }

  private async gerarAbastecimentoDaNota(
    manager: EntityManager,
    idEmpresa: number,
    idNotaFiscal: number,
    payload: PayloadNotaFiscalNormalizado,
  ): Promise<void> {
    if (!payload.idVeiculo || !payload.idFornecedor || payload.kmInformado === null) {
      return;
    }

    const itemPrincipal = this.resolverItemPrincipalParaAbastecimento(payload.itens);
    if (!itemPrincipal) {
      return;
    }

    const colunas = await this.carregarColunasAbastecimentoNota(manager);
    const campos: string[] = [];
    const valores: Array<string | number | Date | null> = [];

    const adicionar = (coluna: string | null, valor: string | number | Date | null) => {
      if (!coluna) return;
      valores.push(valor);
      campos.push(`${this.quote(coluna)} = $${valores.length}`);
    };

    const inserirCampos: string[] = [];
    const placeholders: string[] = [];
    const pushCampo = (coluna: string | null, valor: string | number | Date | null) => {
      if (!coluna) return;
      valores.push(valor);
      inserirCampos.push(this.quote(coluna));
      placeholders.push(`$${valores.length}`);
    };

    valores.length = 0;
    pushCampo(colunas.idVeiculo, payload.idVeiculo);
    pushCampo(colunas.idFornecedor, payload.idFornecedor);
    pushCampo(colunas.dataAbastecimento, payload.dataAbastecimento ?? payload.dataEmissao);
    pushCampo(colunas.litros, itemPrincipal.litros);
    pushCampo(colunas.valorLitro, itemPrincipal.valorUnitario);
    pushCampo(colunas.km, payload.kmInformado);

    if (colunas.idViagem) {
      pushCampo(colunas.idViagem, payload.idViagem);
    }

    if (colunas.valorTotal) {
      pushCampo(colunas.valorTotal, itemPrincipal.valorTotal);
    }

    if (colunas.observacao) {
      pushCampo(
        colunas.observacao,
        this.limitarTexto(
          this.juntarTextos(
            'GERADO AUTOMATICAMENTE PELA NOTA FISCAL',
            payload.observacoes,
          ),
          2000,
        ),
      );
    }

    if (colunas.usuarioAtualizacao) {
      pushCampo(colunas.usuarioAtualizacao, payload.usuarioAtualizacao);
    }

    if (colunas.origemLancamento) {
      pushCampo(colunas.origemLancamento, 'NOTA_FISCAL');
    }

    if (colunas.idNotaFiscal) {
      pushCampo(colunas.idNotaFiscal, idNotaFiscal);
    }

    if (colunas.idEmpresa) {
      pushCampo(colunas.idEmpresa, String(idEmpresa));
    }

    await manager.query(
      `
        INSERT INTO app.abastecimentos (${inserirCampos.join(', ')})
        VALUES (${placeholders.join(', ')})
      `,
      valores,
    );
  }

  private resolverItemPrincipalParaAbastecimento(
    itens: ItemNotaFiscalNormalizado[],
  ) {
    const candidatos = itens.filter((item) => item.litros > 0);
    if (candidatos.length === 0) {
      return null;
    }

    const naoArla = candidatos.filter(
      (item) => (item.combustivel ?? '').toUpperCase() !== 'ARLA',
    );
    const base = naoArla.length > 0 ? naoArla : candidatos;
    return [...base].sort((a, b) => b.litros - a.litros || b.valorTotal - a.valorTotal)[0] ?? null;
  }

  private async carregarColunasAbastecimentoNota(
    manager: EntityManager,
  ): Promise<ColunasAbastecimentoNota> {
    const rows = (await manager.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'app'
        AND table_name = 'abastecimentos'
    `)) as Array<{ column_name?: string }>;

    const set = new Set(
      rows
        .map((row) => (typeof row.column_name === 'string' ? row.column_name : ''))
        .filter(Boolean),
    );

    return {
      idEmpresa: this.findColumn(set, ['id_empresa'], false),
      idVeiculo: this.findColumn(set, ['id_veiculo', 'veiculo_id'], true)!,
      idFornecedor: this.findColumn(set, ['id_fornecedor', 'fornecedor_id'], true)!,
      idViagem: this.findColumn(set, ['id_viagem', 'viagem_id'], false),
      dataAbastecimento: this.findColumn(set, ['data_abastecimento', 'data', 'data_lancamento', 'dt_abastecimento'], true)!,
      litros: this.findColumn(set, ['litros', 'quantidade_litros', 'qtd_litros'], true)!,
      valorLitro: this.findColumn(set, ['valor_litro', 'preco_litro', 'vl_litro'], true)!,
      valorTotal: this.findColumn(set, ['valor_total', 'total', 'valor'], false),
      km: this.findColumn(set, ['km', 'km_abastecimento', 'km_veiculo', 'km_atual'], true)!,
      observacao: this.findColumn(set, ['observacao', 'observacoes', 'obs'], false),
      usuarioAtualizacao: this.findColumn(set, ['usuario_atualizacao', 'usuario_update'], false),
      origemLancamento: this.findColumn(set, ['origem_lancamento'], false),
      idNotaFiscal: this.findColumn(set, ['id_nota_fiscal'], false),
    };
  }

  private findColumn(
    set: Set<string>,
    candidatas: string[],
    obrigatoria: boolean,
  ): string | null {
    for (const candidata of candidatas) {
      if (set.has(candidata)) {
        return candidata;
      }
    }

    if (obrigatoria) {
      throw new BadRequestException(
        'Estrutura da tabela app.abastecimentos esta diferente do esperado para o vinculo com nota fiscal.',
      );
    }

    return null;
  }

  private extrairNotaDoXml(xml: string): NotaImportada {
    const blocoIde = this.extrairBloco(xml, 'ide');
    const blocoEmit = this.extrairBloco(xml, 'emit');
    const blocoTotal = this.extrairBloco(xml, 'ICMSTot');
    const blocoInfProt = this.extrairBloco(xml, 'infProt');
    const blocoInfAdic = this.extrairBloco(xml, 'infAdic');

    const numeroNf = this.extrairTag(blocoIde, 'nNF');
    const serieNf = this.extrairTag(blocoIde, 'serie');
    const modeloNf = this.extrairTag(blocoIde, 'mod');
    const dataEmissao =
      this.extrairDataValida(this.extrairTag(blocoIde, 'dhEmi')) ??
      this.extrairDataValida(this.extrairTag(blocoIde, 'dEmi'));

    if (!dataEmissao) {
      throw new BadRequestException(
        'Nao foi possivel localizar a data de emissao no XML.',
      );
    }

    const razaoSocialEmitente = this.extrairTag(blocoEmit, 'xNome');
    if (!razaoSocialEmitente) {
      throw new BadRequestException(
        'Nao foi possivel localizar o emitente no XML.',
      );
    }

    const detalhes = this.extrairItens(xml);
    if (detalhes.length === 0) {
      throw new BadRequestException(
        'Nao foi possivel localizar itens da nota no XML.',
      );
    }

    const placaEncontrada =
      this.extrairPlacaDoXml(xml) ?? this.extrairPlacaDeTextoLivre(blocoInfAdic);
    const kmInformado = this.extrairKmDoTexto(blocoInfAdic);

    const totalLitrosItens = detalhes.reduce((acc, item) => acc + item.litros, 0);
    const totalProdutosItens = detalhes.reduce(
      (acc, item) => acc + item.valorTotal,
      0,
    );
    const totalDescontoItens = detalhes.reduce(
      (acc, item) => acc + item.valorDesconto,
      0,
    );

    const chaveDoId = this.extrairAtributo(xml, 'infNFe', 'Id');
    const chaveAcesso =
      this.extrairSomenteDigitos(this.extrairTag(blocoInfProt, 'chNFe'), 44) ??
      this.extrairSomenteDigitos(chaveDoId, 44);

    const observacoes = this.limitarTexto(
      this.juntarTextos(
        this.extrairTag(blocoInfAdic, 'infCpl'),
        this.extrairTag(blocoInfAdic, 'infAdFisco'),
      ),
      2000,
    );

    return {
      chaveAcesso,
      numeroNf,
      serieNf,
      modeloNf,
      dataEmissao,
      dataAbastecimento: dataEmissao,
      protocoloAutorizacao: this.extrairTag(blocoInfProt, 'nProt'),
      cnpjEmitente: this.extrairSomenteDigitos(
        this.extrairTag(blocoEmit, 'CNPJ'),
        14,
      ),
      razaoSocialEmitente,
      nomeFantasiaEmitente: this.extrairTag(blocoEmit, 'xFant'),
      ufEmitente: this.normalizarTextoCurto(this.extrairTag(blocoEmit, 'UF')),
      placaVeiculoInformada: placaEncontrada,
      kmInformado,
      totalLitros: Number(totalLitrosItens.toFixed(3)),
      valorTotalProdutos:
        this.toMoneyNumber(this.extrairTag(blocoTotal, 'vProd')) ??
        Number(totalProdutosItens.toFixed(2)),
      valorTotalDesconto:
        this.toMoneyNumber(this.extrairTag(blocoTotal, 'vDesc')) ??
        Number(totalDescontoItens.toFixed(2)),
      valorTotalNota:
        this.toMoneyNumber(this.extrairTag(blocoTotal, 'vNF')) ??
        Number(totalProdutosItens.toFixed(2)),
      valorTotalIcms:
        this.toMoneyNumber(this.extrairTag(blocoTotal, 'vICMS')) ?? 0,
      observacoes,
      itens: detalhes,
      xmlExtraidoJson: {
        xmlProcessadoEm: new Date().toISOString(),
        emitente: {
          cnpj: this.extrairSomenteDigitos(this.extrairTag(blocoEmit, 'CNPJ'), 14),
          razaoSocial: razaoSocialEmitente,
          nomeFantasia: this.extrairTag(blocoEmit, 'xFant'),
          uf: this.normalizarTextoCurto(this.extrairTag(blocoEmit, 'UF')),
        },
        identificacao: {
          numeroNf,
          serieNf,
          modeloNf,
          chaveAcesso,
          protocoloAutorizacao: this.extrairTag(blocoInfProt, 'nProt'),
          dataEmissao,
        },
        abastecimento: {
          placaVeiculoInformada: placaEncontrada,
          kmInformado,
          totalLitros: Number(totalLitrosItens.toFixed(3)),
        },
        itens: detalhes,
      },
    };
  }

  private extrairItens(xml: string): NotaImportada['itens'] {
    const regex =
      /<(?:[\w-]+:)?det\b([^>]*)>([\s\S]*?)<\/(?:[\w-]+:)?det>/gi;
    const itens: NotaImportada['itens'] = [];
    let match: RegExpExecArray | null = regex.exec(xml);

    while (match) {
      const atributos = match[1] ?? '';
      const bloco = match[2] ?? '';
      const blocoProd = this.extrairBloco(bloco, 'prod');
      const blocoComb = this.extrairBloco(bloco, 'comb');

      const numeroItem =
        this.toNumber(this.extrairAtributoBruto(atributos, 'nItem')) ??
        itens.length + 1;
      const descricaoProduto = this.extrairTag(blocoProd, 'xProd');
      if (descricaoProduto) {
        const quantidade = this.toMoneyNumber(this.extrairTag(blocoProd, 'qCom')) ?? 0;
        const valorUnitario =
          this.toMoneyNumber(this.extrairTag(blocoProd, 'vUnCom')) ?? 0;
        const valorDesconto =
          this.toMoneyNumber(this.extrairTag(blocoProd, 'vDesc')) ?? 0;
        const valorTotal =
          this.toMoneyNumber(this.extrairTag(blocoProd, 'vProd')) ?? 0;

        itens.push({
          numeroItem,
          codigoProduto: this.extrairTag(blocoProd, 'cProd'),
          descricaoProduto,
          codigoAnp:
            this.extrairTag(blocoComb, 'cProdANP') ??
            this.extrairTag(blocoProd, 'cProdANP'),
          ncm: this.extrairTag(blocoProd, 'NCM'),
          cfop: this.extrairTag(blocoProd, 'CFOP'),
          unidade: this.extrairTag(blocoProd, 'uCom'),
          quantidade,
          litros: Number(quantidade.toFixed(3)),
          valorUnitario,
          valorDesconto,
          valorTotal,
          combustivel: this.normalizarCombustivel(descricaoProduto),
        });
      }

      match = regex.exec(xml);
    }

    return itens;
  }

  private mapearNota(
    row: RegistroBanco,
    itens: NotaFiscalAbastecimentoItem[],
  ): NotaFiscalAbastecimento {
    return {
      idNotaFiscal: this.toNumber(row.id_nota_fiscal) ?? 0,
      idEmpresa: this.toNumber(row.id_empresa) ?? 0,
      idFornecedor: this.toNumber(row.id_fornecedor),
      idVeiculo: this.toNumber(row.id_veiculo),
      idMotorista: this.toNumber(row.id_motorista),
      idViagem: this.toNumber(row.id_viagem),
      chaveAcesso: this.toText(row.chave_acesso),
      numeroNf: this.toText(row.numero_nf),
      serieNf: this.toText(row.serie_nf),
      modeloNf: this.toText(row.modelo_nf),
      origemLancamento: this.toText(row.origem_lancamento) ?? 'MANUAL',
      statusDocumento:
        this.toText(row.status_documento) ??
        this.mapearStatusDocumentoPorStatusFiscal(
          this.toText(row.status_fiscal) ?? 'PENDENTE',
        ),
      statusFiscal: this.toText(row.status_fiscal) ?? 'PENDENTE',
      statusOperacional: this.toText(row.status_operacional) ?? 'IMPORTADA',
      dataEmissao: this.toDate(row.data_emissao) ?? new Date(0),
      dataAbastecimento: this.toDate(row.data_abastecimento),
      dataEntrada: this.toDate(row.data_entrada),
      protocoloAutorizacao: this.toText(row.protocolo_autorizacao),
      cnpjEmitente: this.toText(row.cnpj_emitente),
      razaoSocialEmitente: this.toText(row.razao_social_emitente) ?? '',
      nomeFantasiaEmitente: this.toText(row.nome_fantasia_emitente),
      ufEmitente: this.toText(row.uf_emitente),
      placaVeiculoInformada: this.toText(row.placa_veiculo_informada),
      kmInformado: this.toNumber(row.km_informado),
      totalLitros: this.toNumber(row.total_litros) ?? 0,
      valorTotalProdutos: this.toNumber(row.valor_total_produtos) ?? 0,
      valorTotalDesconto: this.toNumber(row.valor_total_desconto) ?? 0,
      valorTotalNota: this.toNumber(row.valor_total_nota) ?? 0,
      valorTotalIcms: this.toNumber(row.valor_total_icms) ?? 0,
      observacoes: this.toText(row.observacoes),
      motivoStatus: this.toText(row.motivo_status),
      efetivadaEm: this.toDate(row.efetivada_em),
      efetivadaPor: this.toText(row.efetivada_por),
      xmlOriginal: this.toText(row.xml_original),
      xmlExtraidoJson:
        this.toJsonObject(row.xml_extraido_json) ?? ({} as Record<string, unknown>),
      usuarioAtualizacao: this.toText(row.usuario_atualizacao),
      criadoEm: this.toDate(row.criado_em),
      atualizadoEm: this.toDate(row.atualizado_em),
      itens,
    };
  }

  private mapearItem(row: RegistroBanco): NotaFiscalAbastecimentoItem {
    return {
      idItemNotaFiscal: this.toNumber(row.id_item_nota_fiscal) ?? 0,
      idNotaFiscal: this.toNumber(row.id_nota_fiscal) ?? 0,
      idEmpresa: this.toNumber(row.id_empresa) ?? 0,
      idAbastecimento: this.toNumber(row.id_abastecimento),
      idProduto: this.toNumber(row.id_produto),
      numeroItem: this.toNumber(row.numero_item) ?? 0,
      codigoProduto: this.toText(row.codigo_produto),
      codigoProdutoXml: this.toText(row.codigo_produto_xml),
      descricaoProduto: this.toText(row.descricao_produto) ?? '',
      codigoAnp: this.toText(row.codigo_anp),
      ncm: this.toText(row.ncm),
      cfop: this.toText(row.cfop),
      unidade: this.toText(row.unidade),
      quantidade: this.toNumber(row.quantidade) ?? 0,
      litros: this.toNumber(row.litros) ?? 0,
      valorUnitario: this.toNumber(row.valor_unitario) ?? 0,
      valorDesconto: this.toNumber(row.valor_desconto) ?? 0,
      valorTotal: this.toNumber(row.valor_total) ?? 0,
      combustivel: this.toText(row.combustivel),
      observacoes: this.toText(row.observacoes),
      usuarioAtualizacao: this.toText(row.usuario_atualizacao),
      criadoEm: this.toDate(row.criado_em),
      atualizadoEm: this.toDate(row.atualizado_em),
    };
  }

  private extrairBloco(xml: string, tag: string): string {
    const regex = new RegExp(
      `<(?:[\\w-]+:)?${tag}(?:\\b[^>]*)>([\\s\\S]*?)<\\/(?:[\\w-]+:)?${tag}>`,
      'i',
    );
    const match = regex.exec(xml);
    return match?.[1] ?? '';
  }

  private extrairTag(xml: string, tag: string): string | null {
    if (!xml) {
      return null;
    }

    const regex = new RegExp(
      `<(?:[\\w-]+:)?${tag}(?:\\b[^>]*)>([\\s\\S]*?)<\\/(?:[\\w-]+:)?${tag}>`,
      'i',
    );
    const match = regex.exec(xml);
    return this.limparTextoXml(match?.[1] ?? null);
  }

  private extrairAtributo(
    xml: string,
    tag: string,
    atributo: string,
  ): string | null {
    const regex = new RegExp(`<(?:[\\w-]+:)?${tag}\\b([^>]*)>`, 'i');
    const match = regex.exec(xml);
    return this.extrairAtributoBruto(match?.[1] ?? '', atributo);
  }

  private extrairAtributoBruto(
    atributos: string,
    atributo: string,
  ): string | null {
    const regex = new RegExp(`${atributo}\\s*=\\s*["']([^"']+)["']`, 'i');
    const match = regex.exec(atributos);
    return this.limparTextoXml(match?.[1] ?? null);
  }

  private extrairPlacaDoXml(xml: string): string | null {
    const candidatos = [
      this.extrairTag(xml, 'placa'),
      this.extrairTag(xml, 'placaVeic'),
      this.extrairTag(xml, 'veicPlaca'),
    ];

    for (const candidato of candidatos) {
      const placa = this.normalizarPlaca(candidato ?? '');
      if (placa) {
        return placa;
      }
    }

    return null;
  }

  private extrairPlacaDeTextoLivre(texto: string): string | null {
    if (!texto) {
      return null;
    }

    const match = texto
      .toUpperCase()
      .replace(/[^A-Z0-9]/g, ' ')
      .match(/\b[A-Z]{3}[0-9][A-Z0-9][0-9]{2}\b/);
    return match ? this.normalizarPlaca(match[0]) : null;
  }

  private extrairKmDoTexto(texto: string): number | null {
    if (!texto) {
      return null;
    }

    const match = texto.match(/\bKM\s*[:=-]?\s*(\d{1,9})\b/i);
    return match ? this.toNumber(match[1]) : null;
  }

  private extrairDataValida(valor: string | null): string | null {
    if (!valor) {
      return null;
    }

    const data = new Date(valor);
    return Number.isNaN(data.getTime()) ? null : data.toISOString();
  }

  private normalizarCombustivel(descricao: string): string | null {
    const texto = descricao.trim().toUpperCase();
    if (!texto) {
      return null;
    }
    if (texto.includes('DIESEL')) return 'DIESEL';
    if (texto.includes('GASOLINA')) return 'GASOLINA';
    if (texto.includes('ETANOL') || texto.includes('ALCOOL')) return 'ETANOL';
    if (texto.includes('ARLA')) return 'ARLA';
    if (texto.includes('GNV')) return 'GNV';
    return texto.slice(0, 40);
  }

  private normalizarXml(xml: string): string {
    return xml.replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').trim();
  }

  private limparTextoXml(valor: string | null): string | null {
    if (!valor) {
      return null;
    }

    const texto = valor
      .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
      .replace(/&amp;/g, '&')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"')
      .replace(/&apos;/g, "'")
      .replace(/\s+/g, ' ')
      .trim();

    return texto || null;
  }

  private executarComRls<T>(
    idEmpresa: number,
    callback: (manager: EntityManager) => Promise<T>,
  ): Promise<T> {
    return this.dataSource.manager.transaction(async (manager) => {
      await configurarContextoEmpresaRls(manager, idEmpresa);
      return callback(manager);
    });
  }

  private tratarErroPersistencia(error: unknown, acao: string): never {
    if (error instanceof BadRequestException || error instanceof NotFoundException) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as { code?: string; message?: string; detail?: string };
      if (erroPg.code === '23505') {
        throw new BadRequestException(
          'Ja existe uma nota fiscal com a mesma chave de acesso ou identificacao para esta empresa.',
        );
      }

      if (erroPg.code === '23503') {
        throw new BadRequestException(
          'Fornecedor, veiculo ou motorista informado nao pertence a empresa logada.',
        );
      }

      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Usuario do banco sem permissao para gravar notas fiscais.',
        );
      }

      if (erroPg.code === '22P02' || erroPg.code === '22007') {
        throw new BadRequestException(
          'Formato de numero ou data invalido ao salvar a nota fiscal.',
        );
      }

      if (erroPg.code === '42703') {
        throw new BadRequestException(
          'Estrutura da tabela de nota fiscal esta desatualizada. Execute o script sql/nf_abastecimento_vinculos_viagem_produto.sql no banco e tente novamente.',
        );
      }

      if (erroPg.code === '42P01') {
        throw new BadRequestException(
          'Tabela de nota fiscal nao encontrada no banco. Verifique se os scripts de schema foram executados.',
        );
      }

      if (erroPg.code === '42601') {
        throw new BadRequestException(
          'Erro SQL ao salvar a nota fiscal. Verifique se a estrutura do banco esta sincronizada com a ultima versao do sistema.',
        );
      }

      const detalhe = [erroPg.code, erroPg.detail ?? erroPg.message]
        .filter((parte): parte is string => Boolean(parte))
        .join(' - ');
      if (detalhe) {
        throw new BadRequestException(
          `Falha ao ${acao}: ${detalhe}.`,
        );
      }
    }

    throw new BadRequestException(`Nao foi possivel ${acao} neste momento.`);
  }

  private normalizarUsuario(valor: string): string {
    return valor.trim().toUpperCase();
  }

  private normalizarTextoObrigatorio(valor: string): string {
    const texto = valor.trim();
    if (!texto) {
      throw new BadRequestException('Campo obrigatorio nao informado.');
    }
    return texto.toUpperCase();
  }

  private normalizarTextoCurto(valor: string | null): string | null {
    return valor?.trim().toUpperCase() ?? null;
  }

  private nullIfBlank(valor: string | null | undefined): string | null {
    if (valor === null || valor === undefined) {
      return null;
    }
    const texto = valor.trim();
    return texto ? texto : null;
  }

  private limitarTexto(valor: string | null, tamanho: number): string | null {
    if (!valor) {
      return null;
    }
    return valor.length <= tamanho ? valor : valor.slice(0, tamanho);
  }

  private juntarTextos(
    primeiro: string | null,
    segundo: string | null,
  ): string | null {
    const partes = [primeiro, segundo]
      .filter((item): item is string => Boolean(item))
      .map((item) => item.trim())
      .filter(Boolean);
    return partes.length > 0 ? partes.join(' | ') : null;
  }

  private normalizarPlaca(valor: string): string | null {
    const texto = valor.toUpperCase().replace(/[^A-Z0-9]/g, '');
    return texto.length >= 7 ? texto.slice(0, 10) : null;
  }

  private extrairSomenteDigitos(
    valor: string | null,
    tamanhoEsperado?: number,
  ): string | null {
    if (!valor) {
      return null;
    }

    const digitos = valor.replace(/\D/g, '');
    if (!digitos) {
      return null;
    }

    if (tamanhoEsperado && digitos.length < tamanhoEsperado) {
      return null;
    }

    return tamanhoEsperado ? digitos.slice(-tamanhoEsperado) : digitos;
  }

  private toMoneyNumber(valor: string | null): number | null {
    if (!valor) {
      return null;
    }
    const numero = Number(valor.replace(',', '.'));
    return Number.isFinite(numero) ? numero : null;
  }

  private fix2(valor: number): number {
    return Number((Number.isFinite(valor) ? valor : 0).toFixed(2));
  }

  private fix3(valor: number): number {
    return Number((Number.isFinite(valor) ? valor : 0).toFixed(3));
  }

  private fix6(valor: number): number {
    return Number((Number.isFinite(valor) ? valor : 0).toFixed(6));
  }

  private toNumber(valor: unknown): number | null {
    if (valor === null || valor === undefined) {
      return null;
    }
    const numero = Number(valor);
    return Number.isFinite(numero) ? numero : null;
  }

  private toText(valor: unknown): string | null {
    if (typeof valor !== 'string') {
      return null;
    }
    const texto = valor.trim();
    return texto ? texto : null;
  }

  private toDate(valor: unknown): Date | null {
    if (valor === null || valor === undefined) {
      return null;
    }
    const data = new Date(
      valor instanceof Date || typeof valor === 'string' || typeof valor === 'number'
        ? valor
        : '',
    );
    return Number.isNaN(data.getTime()) ? null : data;
  }

  private toJsonObject(valor: unknown): Record<string, unknown> | null {
    if (!valor) {
      return null;
    }

    if (typeof valor === 'object' && !Array.isArray(valor)) {
      return valor as Record<string, unknown>;
    }

    if (typeof valor === 'string') {
      try {
        const parsed = JSON.parse(valor) as unknown;
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          return parsed as Record<string, unknown>;
        }
      } catch {
        return null;
      }
    }

    return null;
  }

  private quote(coluna: string): string {
    if (!/^[a-z_][a-z0-9_]*$/.test(coluna)) {
      throw new BadRequestException(
        `Nome de coluna invalido detectado: ${coluna}`,
      );
    }

    return `"${coluna}"`;
  }
}
