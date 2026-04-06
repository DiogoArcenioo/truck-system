import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { EntityManager, QueryFailedError, Repository, SelectQueryBuilder } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { AtualizarMotoristaDto } from './dto/atualizar-motorista.dto';
import { CriarMotoristaDto } from './dto/criar-motorista.dto';
import { FiltroMotoristasDto } from './dto/filtro-motoristas.dto';
import {
  ListarMotoristaDto,
  ListarMotoristaEnderecoDto,
} from './dto/listar-motorista.dto';
import {
  AtualizarMotoristaEnderecoDto,
  CriarMotoristaEnderecoDto,
} from './dto/motorista-endereco.dto';
import { MotoristaEntity } from './entities/motorista.entity';
import {
  CATEGORIAS_CNH_OPCOES,
  STATUS_MOTORISTA_LABEL_POR_CODIGO,
  STATUS_MOTORISTA_OPCOES,
} from './motoristas.constants';

type RegistroBanco = Record<string, unknown>;

type PayloadEnderecoMotorista = {
  logradouro: string;
  numero: string | null;
  complemento: string | null;
  bairro: string | null;
  cidade: string | null;
  estado: string | null;
  cep: string | null;
  principal: boolean;
  usuarioAtualizacao: string;
};

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
    return this.executarComRls(idEmpresa, async (motoristaRepository, manager) => {
      const motoristas = await motoristaRepository.find({
        where: { idEmpresa: String(idEmpresa) },
        order: { nome: 'ASC', idMotorista: 'ASC' },
      });

      const dados: ListarMotoristaDto[] = [];

      for (const motorista of motoristas) {
        const qtdEnderecos = await this.contarEnderecos(manager, motorista.idMotorista);
        dados.push(this.mapearMotorista(motorista, qtdEnderecos));
      }

      return {
        sucesso: true,
        total: dados.length,
        motoristas: dados,
      };
    });
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroMotoristasDto) {
    this.validarIntervalosDoFiltro(filtro);

    return this.executarComRls(idEmpresa, async (motoristaRepository, manager) => {
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
      const dados: ListarMotoristaDto[] = [];

      for (const motorista of motoristas) {
        const qtdEnderecos = await this.contarEnderecos(manager, motorista.idMotorista);
        dados.push(this.mapearMotorista(motorista, qtdEnderecos));
      }

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
    return this.executarComRls(idEmpresa, async (motoristaRepository, manager) => {
      const motorista = await motoristaRepository.findOne({
        where: { idEmpresa: String(idEmpresa), idMotorista },
      });

      if (!motorista) {
        throw new NotFoundException('Motorista nao encontrado para a empresa logada.');
      }

      const enderecos = await this.listarEnderecosMotorista(manager, idEmpresa, idMotorista);

      return {
        sucesso: true,
        motorista: this.mapearMotorista(motorista, enderecos.length, enderecos),
      };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarMotoristaDto,
    _usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (motoristaRepository) => {
        this.validarCamposObrigatoriosCriacao(dados);
        this.validarDocumentoCpf(dados.cpf);

        const dataNascimento = this.normalizarDataObrigatoria(
          dados.dataNascimento,
          'data de nascimento',
        );
        const telefone1 = this.normalizarTelefoneObrigatorio(
          dados.telefone1,
          'telefone 1',
        );
        const telefone2 = this.normalizarTelefoneOpcionalValidado(
          dados.telefone2,
          'telefone 2',
        );
        const email = this.normalizarTextoOpcional(dados.email, 'email', false);
        const dataAdmissao = this.normalizarDataOpcional(
          dados.dataAdmissao,
          'data de admissao',
        );
        const dataDemissao = this.normalizarDataOpcional(
          dados.dataDemissao,
          'data de demissao',
        );

        this.validarEmailOpcional(email);
        this.validarConsistenciaDatas(dataNascimento, dataAdmissao, dataDemissao);

        const novo = motoristaRepository.create({
          idEmpresa: String(idEmpresa),
          nome: this.normalizarTexto(dados.nome),
          cpf: dados.cpf,
          cnh: this.normalizarTexto(dados.cnh),
          dataNascimento,
          email,
          telefone1,
          telefone2,
          categoriaCnh: dados.categoriaCnh,
          validadeCnh: this.normalizarData(dados.validadeCnh),
          dataAdmissao,
          dataDemissao,
          tipoContrato: this.normalizarTextoOpcional(
            dados.tipoContrato,
            'tipo de contrato',
          ),
          status: 'A',
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
      return await this.executarComRls(idEmpresa, async (motoristaRepository) => {
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
          this.validarDocumentoCpf(dados.cpf);
          motorista.cpf = dados.cpf;
        }

        if (dados.cnh !== undefined) {
          motorista.cnh = this.normalizarTexto(dados.cnh);
        }

        if (dados.dataNascimento !== undefined) {
          motorista.dataNascimento = this.normalizarDataOpcional(
            dados.dataNascimento,
            'data de nascimento',
          );
        }

        if (dados.email !== undefined) {
          motorista.email = this.normalizarTextoOpcional(dados.email, 'email', false);
        }

        if (dados.telefone1 !== undefined) {
          motorista.telefone1 = this.normalizarTelefoneObrigatorio(
            dados.telefone1,
            'telefone 1',
          );
        }

        if (dados.telefone2 !== undefined) {
          motorista.telefone2 = this.normalizarTelefoneOpcionalValidado(
            dados.telefone2,
            'telefone 2',
          );
        }

        if (dados.categoriaCnh !== undefined) {
          motorista.categoriaCnh = dados.categoriaCnh;
        }

        if (dados.validadeCnh !== undefined) {
          motorista.validadeCnh = this.normalizarData(dados.validadeCnh);
        }

        if (dados.dataAdmissao !== undefined) {
          motorista.dataAdmissao = this.normalizarDataOpcional(
            dados.dataAdmissao,
            'data de admissao',
          );
        }

        if (dados.dataDemissao !== undefined) {
          motorista.dataDemissao = this.normalizarDataOpcional(
            dados.dataDemissao,
            'data de demissao',
          );
        }

        if (dados.tipoContrato !== undefined) {
          motorista.tipoContrato = this.normalizarTextoOpcional(
            dados.tipoContrato,
            'tipo de contrato',
          );
        }

        if (dados.status !== undefined) {
          motorista.status = dados.status;
        }

        this.validarEmailOpcional(motorista.email);
        this.validarConsistenciaDatas(
          motorista.dataNascimento,
          motorista.dataAdmissao,
          motorista.dataDemissao,
        );

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

  async adicionarEndereco(
    idEmpresa: number,
    idMotorista: number,
    dados: CriarMotoristaEnderecoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (motoristaRepository, manager) => {
        await this.buscarMotoristaOuFalhar(motoristaRepository, idEmpresa, idMotorista);
        const payload = this.normalizarEnderecoCriacao(dados, usuarioJwt);

        const rows = (await manager.query(
          `
            INSERT INTO app.motorista_enderecos (
              id_motorista, id_empresa, logradouro, numero, complemento, bairro, cidade,
              estado, cep, principal, usuario_atualizacao, criado_em, atualizado_em
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW(),NOW())
            RETURNING id_endereco
          `,
          [
            idMotorista,
            String(idEmpresa),
            payload.logradouro,
            payload.numero,
            payload.complemento,
            payload.bairro,
            payload.cidade,
            payload.estado,
            payload.cep,
            payload.principal,
            payload.usuarioAtualizacao,
          ],
        )) as Array<{ id_endereco?: string | number }>;

        const idEndereco = this.converterNumero(rows[0]?.id_endereco);
        if (!idEndereco) {
          throw new BadRequestException('Falha ao cadastrar endereco do motorista.');
        }

        if (payload.principal) {
          await this.marcarEnderecoPrincipal(manager, idMotorista, idEndereco);
        } else {
          await this.garantirEnderecoPrincipal(manager, idMotorista);
        }

        return {
          sucesso: true,
          mensagem: 'Endereco cadastrado com sucesso.',
          motorista: await this.carregarMotoristaCompleto(
            motoristaRepository,
            manager,
            idEmpresa,
            idMotorista,
          ),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar endereco');
    }
  }

  async atualizarEndereco(
    idEmpresa: number,
    idMotorista: number,
    idEndereco: number,
    dados: AtualizarMotoristaEnderecoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (motoristaRepository, manager) => {
        await this.buscarMotoristaOuFalhar(motoristaRepository, idEmpresa, idMotorista);
        const atual = await this.buscarEnderecoOuFalhar(manager, idEmpresa, idMotorista, idEndereco);
        const payload = this.normalizarEnderecoAtualizacao(dados, usuarioJwt, atual);

        await manager.query(
          `
            UPDATE app.motorista_enderecos
            SET
              logradouro = $1,
              numero = $2,
              complemento = $3,
              bairro = $4,
              cidade = $5,
              estado = $6,
              cep = $7,
              principal = $8,
              usuario_atualizacao = $9,
              atualizado_em = NOW()
            WHERE id_motorista = $10
              AND id_empresa = $11
              AND id_endereco = $12
          `,
          [
            payload.logradouro,
            payload.numero,
            payload.complemento,
            payload.bairro,
            payload.cidade,
            payload.estado,
            payload.cep,
            payload.principal,
            payload.usuarioAtualizacao,
            idMotorista,
            String(idEmpresa),
            idEndereco,
          ],
        );

        if (payload.principal) {
          await this.marcarEnderecoPrincipal(manager, idMotorista, idEndereco);
        } else {
          await this.garantirEnderecoPrincipal(manager, idMotorista);
        }

        return {
          sucesso: true,
          mensagem: 'Endereco atualizado com sucesso.',
          motorista: await this.carregarMotoristaCompleto(
            motoristaRepository,
            manager,
            idEmpresa,
            idMotorista,
          ),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar endereco');
    }
  }

  async removerEndereco(
    idEmpresa: number,
    idMotorista: number,
    idEndereco: number,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (motoristaRepository, manager) => {
        await this.buscarMotoristaOuFalhar(motoristaRepository, idEmpresa, idMotorista);
        await this.buscarEnderecoOuFalhar(manager, idEmpresa, idMotorista, idEndereco);

        await manager.query(
          `
            DELETE FROM app.motorista_enderecos
            WHERE id_motorista = $1
              AND id_empresa = $2
              AND id_endereco = $3
          `,
          [idMotorista, String(idEmpresa), idEndereco],
        );

        await this.garantirEnderecoPrincipal(manager, idMotorista);

        return {
          sucesso: true,
          mensagem: 'Endereco removido com sucesso.',
          motorista: await this.carregarMotoristaCompleto(
            motoristaRepository,
            manager,
            idEmpresa,
            idMotorista,
          ),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'remover endereco');
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

  private async carregarMotoristaCompleto(
    motoristaRepository: Repository<MotoristaEntity>,
    manager: EntityManager,
    idEmpresa: number,
    idMotorista: number,
  ) {
    const motorista = await this.buscarMotoristaOuFalhar(
      motoristaRepository,
      idEmpresa,
      idMotorista,
    );
    const enderecos = await this.listarEnderecosMotorista(manager, idEmpresa, idMotorista);
    return this.mapearMotorista(motorista, enderecos.length, enderecos);
  }

  private async buscarMotoristaOuFalhar(
    motoristaRepository: Repository<MotoristaEntity>,
    idEmpresa: number,
    idMotorista: number,
  ) {
    const motorista = await motoristaRepository.findOne({
      where: { idEmpresa: String(idEmpresa), idMotorista },
    });

    if (!motorista) {
      throw new NotFoundException('Motorista nao encontrado para a empresa logada.');
    }

    return motorista;
  }

  private async listarEnderecosMotorista(
    manager: EntityManager,
    idEmpresa: number,
    idMotorista: number,
  ): Promise<ListarMotoristaEnderecoDto[]> {
    const rows = (await manager.query(
      `
        SELECT *
        FROM app.motorista_enderecos
        WHERE id_motorista = $1
          AND id_empresa = $2
        ORDER BY principal DESC, id_endereco ASC
      `,
      [idMotorista, String(idEmpresa)],
    )) as RegistroBanco[];

    return rows.map((row) => this.mapearEndereco(row));
  }

  private async contarEnderecos(manager: EntityManager, idMotorista: number) {
    const rows = (await manager.query(
      `
        SELECT COUNT(1)::int AS total
        FROM app.motorista_enderecos
        WHERE id_motorista = $1
      `,
      [idMotorista],
    )) as Array<{ total?: number | string }>;

    return this.converterNumero(rows[0]?.total) ?? 0;
  }

  private async buscarEnderecoOuFalhar(
    manager: EntityManager,
    idEmpresa: number,
    idMotorista: number,
    idEndereco: number,
  ) {
    const rows = (await manager.query(
      `
        SELECT *
        FROM app.motorista_enderecos
        WHERE id_motorista = $1
          AND id_empresa = $2
          AND id_endereco = $3
        LIMIT 1
      `,
      [idMotorista, String(idEmpresa), idEndereco],
    )) as RegistroBanco[];

    if (!rows[0]) {
      throw new NotFoundException('Endereco do motorista nao encontrado.');
    }

    return rows[0];
  }

  private async marcarEnderecoPrincipal(
    manager: EntityManager,
    idMotorista: number,
    idEnderecoPrincipal: number,
  ) {
    await manager.query(
      `
        UPDATE app.motorista_enderecos
        SET principal = CASE WHEN id_endereco = $2 THEN true ELSE false END,
            atualizado_em = NOW()
        WHERE id_motorista = $1
      `,
      [idMotorista, idEnderecoPrincipal],
    );
  }

  private async garantirEnderecoPrincipal(manager: EntityManager, idMotorista: number) {
    const rows = (await manager.query(
      `
        SELECT id_endereco, principal
        FROM app.motorista_enderecos
        WHERE id_motorista = $1
        ORDER BY principal DESC, id_endereco ASC
      `,
      [idMotorista],
    )) as Array<{ id_endereco?: number | string; principal?: boolean }>;

    if (rows.length === 0 || rows.some((row) => row.principal === true)) {
      return;
    }

    const primeiro = this.converterNumero(rows[0]?.id_endereco);
    if (!primeiro) {
      return;
    }

    await this.marcarEnderecoPrincipal(manager, idMotorista, primeiro);
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

  private normalizarTexto(valor: string): string {
    const texto = valor.trim().toUpperCase();
    if (!texto) {
      throw new BadRequestException('Texto invalido informado para motorista.');
    }

    return texto;
  }

  private validarCamposObrigatoriosCriacao(dados: CriarMotoristaDto) {
    if (
      !dados.nome?.trim() ||
      !dados.cpf?.trim() ||
      !dados.dataNascimento?.trim() ||
      !dados.telefone1?.trim() ||
      !dados.cnh?.trim() ||
      !dados.categoriaCnh?.trim() ||
      !dados.validadeCnh?.trim()
    ) {
      throw new BadRequestException(
        'Preencha os campos obrigatorios: nome, CPF, data de nascimento, telefone 1, CNH, categoria e vencimento.',
      );
    }
  }

  private validarDocumentoCpf(cpf: string) {
    const digitos = cpf.replace(/\D/g, '').trim();

    if (digitos.length !== 11 || /^(\d)\1{10}$/.test(digitos)) {
      throw new BadRequestException('CPF invalido. Confira os digitos informados.');
    }

    let soma = 0;
    for (let indice = 0; indice < 9; indice += 1) {
      soma += Number(digitos[indice]) * (10 - indice);
    }

    let resto = (soma * 10) % 11;
    if (resto === 10) {
      resto = 0;
    }

    if (resto !== Number(digitos[9])) {
      throw new BadRequestException('CPF invalido. Confira os digitos informados.');
    }

    soma = 0;
    for (let indice = 0; indice < 10; indice += 1) {
      soma += Number(digitos[indice]) * (11 - indice);
    }

    resto = (soma * 10) % 11;
    if (resto === 10) {
      resto = 0;
    }

    if (resto !== Number(digitos[10])) {
      throw new BadRequestException('CPF invalido. Confira os digitos informados.');
    }
  }

  private normalizarTextoOpcional(
    valor: string | null | undefined,
    campo: string,
    upperCase = true,
  ): string | null {
    if (valor === undefined || valor === null) {
      return null;
    }

    const texto = valor.trim();
    if (!texto) {
      return null;
    }

    return upperCase ? texto.toUpperCase() : texto.toLowerCase();
  }

  private normalizarUsuario(valor: string | undefined) {
    const texto = (valor ?? '').trim().toUpperCase();
    return texto || 'APP_WEB';
  }

  private normalizarEnderecoCriacao(
    dados: CriarMotoristaEnderecoDto,
    usuarioJwt: JwtUsuarioPayload,
  ): PayloadEnderecoMotorista {
    return {
      logradouro: this.normalizarTexto(dados.logradouro),
      numero: this.normalizarTextoOpcional(dados.numero, 'numero'),
      complemento: this.normalizarTextoOpcional(dados.complemento, 'complemento'),
      bairro: this.normalizarTextoOpcional(dados.bairro, 'bairro'),
      cidade: this.normalizarTextoOpcional(dados.cidade, 'cidade'),
      estado: this.normalizarEstadoOpcional(dados.estado),
      cep: this.normalizarCepOpcional(dados.cep),
      principal: dados.principal ?? false,
      usuarioAtualizacao: this.normalizarUsuario(usuarioJwt.email),
    };
  }

  private normalizarEnderecoAtualizacao(
    dados: AtualizarMotoristaEnderecoDto,
    usuarioJwt: JwtUsuarioPayload,
    atual: RegistroBanco,
  ): PayloadEnderecoMotorista {
    return {
      logradouro:
        dados.logradouro !== undefined
          ? this.normalizarTexto(dados.logradouro)
          : this.normalizarTexto(String(atual.logradouro ?? '')),
      numero:
        dados.numero !== undefined
          ? this.normalizarTextoOpcional(dados.numero, 'numero')
          : this.normalizarTextoOpcional(this.str(atual.numero), 'numero'),
      complemento:
        dados.complemento !== undefined
          ? this.normalizarTextoOpcional(dados.complemento, 'complemento')
          : this.normalizarTextoOpcional(this.str(atual.complemento), 'complemento'),
      bairro:
        dados.bairro !== undefined
          ? this.normalizarTextoOpcional(dados.bairro, 'bairro')
          : this.normalizarTextoOpcional(this.str(atual.bairro), 'bairro'),
      cidade:
        dados.cidade !== undefined
          ? this.normalizarTextoOpcional(dados.cidade, 'cidade')
          : this.normalizarTextoOpcional(this.str(atual.cidade), 'cidade'),
      estado:
        dados.estado !== undefined
          ? this.normalizarEstadoOpcional(dados.estado)
          : this.normalizarEstadoOpcional(this.str(atual.estado)),
      cep:
        dados.cep !== undefined
          ? this.normalizarCepOpcional(dados.cep)
          : this.normalizarCepOpcional(this.str(atual.cep)),
      principal: dados.principal ?? this.bool(atual.principal) ?? false,
      usuarioAtualizacao: this.normalizarUsuario(usuarioJwt.email),
    };
  }

  private normalizarEstadoOpcional(valor: string | null | undefined) {
    const estado = this.normalizarTextoOpcional(valor, 'estado');
    if (!estado) {
      return null;
    }
    if (estado.length !== 2) {
      throw new BadRequestException('Estado invalido.');
    }
    return estado;
  }

  private normalizarCepOpcional(valor: string | null | undefined) {
    const texto = this.normalizarTextoOpcional(valor, 'cep', false);
    if (!texto) {
      return null;
    }
    const digitos = texto.replace(/\D/g, '');
    if (digitos.length !== 8) {
      throw new BadRequestException('CEP invalido.');
    }
    return digitos;
  }

  private normalizarData(valor: string): string {
    const data = new Date(valor);
    if (Number.isNaN(data.getTime())) {
      throw new BadRequestException('Data invalida informada para validade da CNH.');
    }

    return data.toISOString().slice(0, 10);
  }

  private normalizarDataObrigatoria(
    valor: string | null | undefined,
    descricaoCampo: string,
  ): string {
    const dataNormalizada = this.normalizarDataOpcional(valor, descricaoCampo);
    if (!dataNormalizada) {
      throw new BadRequestException(`Informe ${descricaoCampo}.`);
    }

    return dataNormalizada;
  }

  private normalizarDataOpcional(
    valor: string | null | undefined,
    descricaoCampo: string,
  ): string | null {
    if (valor === undefined || valor === null) {
      return null;
    }

    const texto = valor.trim();
    if (!texto) {
      return null;
    }

    const data = new Date(texto);
    if (Number.isNaN(data.getTime())) {
      throw new BadRequestException(`Data invalida informada para ${descricaoCampo}.`);
    }

    return data.toISOString().slice(0, 10);
  }

  private normalizarTelefoneOpcional(valor: string | null | undefined): string | null {
    if (valor === undefined || valor === null) {
      return null;
    }

    const telefone = valor.replace(/\D/g, '').trim();
    return telefone || null;
  }

  private normalizarTelefoneObrigatorio(
    valor: string | null | undefined,
    descricaoCampo: string,
  ): string {
    const telefone = this.normalizarTelefoneOpcional(valor);
    if (!telefone) {
      throw new BadRequestException(`Informe ${descricaoCampo}.`);
    }

    if (telefone.length !== 10 && telefone.length !== 11) {
      throw new BadRequestException(
        `${descricaoCampo.charAt(0).toUpperCase()}${descricaoCampo.slice(1)} invalido. Informe DDD e numero completos.`,
      );
    }

    return telefone;
  }

  private normalizarTelefoneOpcionalValidado(
    valor: string | null | undefined,
    descricaoCampo: string,
  ): string | null {
    const telefone = this.normalizarTelefoneOpcional(valor);
    if (!telefone) {
      return null;
    }

    if (telefone.length !== 10 && telefone.length !== 11) {
      throw new BadRequestException(
        `${descricaoCampo.charAt(0).toUpperCase()}${descricaoCampo.slice(1)} invalido. Informe DDD e numero completos.`,
      );
    }

    return telefone;
  }

  private validarEmailOpcional(email: string | null) {
    if (!email) {
      return;
    }

    const emailValido = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
    if (!emailValido) {
      throw new BadRequestException('Email invalido. Confira o endereco informado.');
    }
  }

  private validarConsistenciaDatas(
    dataNascimento: string | Date | null | undefined,
    dataAdmissao: string | Date | null | undefined,
    dataDemissao: string | Date | null | undefined,
  ) {
    if (!dataNascimento) {
      return;
    }

    const nascimento = new Date(
      `${this.formatarDataOpcional(dataNascimento)}T00:00:00`,
    );

    if (Number.isNaN(nascimento.getTime())) {
      throw new BadRequestException('Data de nascimento invalida.');
    }

    const hoje = new Date();
    hoje.setHours(0, 0, 0, 0);

    if (nascimento >= hoje) {
      throw new BadRequestException('Data de nascimento deve ser anterior a hoje.');
    }

    if (dataAdmissao) {
      const admissao = new Date(
        `${this.formatarDataOpcional(dataAdmissao)}T00:00:00`,
      );

      if (Number.isNaN(admissao.getTime())) {
        throw new BadRequestException('Data de admissao invalida.');
      }

      if (admissao < nascimento) {
        throw new BadRequestException(
          'Data de admissao nao pode ser anterior a data de nascimento.',
        );
      }
    }

    if (dataAdmissao && dataDemissao) {
      const admissao = new Date(
        `${this.formatarDataOpcional(dataAdmissao)}T00:00:00`,
      );
      const demissao = new Date(
        `${this.formatarDataOpcional(dataDemissao)}T00:00:00`,
      );

      if (demissao < admissao) {
        throw new BadRequestException(
          'Data de demissao deve ser maior ou igual a data de admissao.',
        );
      }
    }
  }

  private formatarDataOpcional(valor: string | Date | null | undefined): string {
    if (!valor) {
      return '';
    }

    return valor instanceof Date ? valor.toISOString().slice(0, 10) : `${valor}`;
  }

  private mapearMotorista(
    motorista: MotoristaEntity,
    qtdEnderecos = 0,
    enderecos?: ListarMotoristaEnderecoDto[],
  ): ListarMotoristaDto {
    const statusNormalizado = (motorista.status ?? '').trim().toUpperCase();
    const validadeCnh = this.formatarDataOpcional(motorista.validadeCnh as unknown as string | Date);

    return {
      idMotorista: motorista.idMotorista,
      nome: this.normalizarTexto(motorista.nome),
      cpf: motorista.cpf?.trim(),
      cnh: this.normalizarTexto(motorista.cnh),
      dataNascimento: this.formatarDataOpcional(motorista.dataNascimento as unknown as string | Date | null),
      email: motorista.email?.trim() ?? '',
      telefone1: motorista.telefone1?.trim() ?? '',
      telefone2: motorista.telefone2?.trim() ?? '',
      categoriaCnh: this.normalizarTexto(motorista.categoriaCnh),
      validadeCnh,
      dataAdmissao: this.formatarDataOpcional(motorista.dataAdmissao as unknown as string | Date | null),
      dataDemissao: this.formatarDataOpcional(motorista.dataDemissao as unknown as string | Date | null),
      tipoContrato: motorista.tipoContrato?.trim() ?? '',
      status: statusNormalizado,
      statusDescricao:
        STATUS_MOTORISTA_LABEL_POR_CODIGO[
          statusNormalizado as keyof typeof STATUS_MOTORISTA_LABEL_POR_CODIGO
        ] ?? statusNormalizado,
      qtdEnderecos,
      enderecos,
    };
  }

  private mapearEndereco(row: RegistroBanco): ListarMotoristaEnderecoDto {
    return {
      idEndereco: this.converterNumero(row.id_endereco) ?? 0,
      idMotorista: this.converterNumero(row.id_motorista) ?? 0,
      logradouro: this.str(row.logradouro) ?? '',
      numero: this.str(row.numero),
      complemento: this.str(row.complemento),
      bairro: this.str(row.bairro),
      cidade: this.str(row.cidade),
      estado: this.str(row.estado),
      cep: this.str(row.cep),
      principal: this.bool(row.principal) ?? false,
      usuarioAtualizacao: this.str(row.usuario_atualizacao),
      criadoEm: this.formatarDataOpcional(row.criado_em as string | Date | null | undefined) || null,
      atualizadoEm: this.formatarDataOpcional(row.atualizado_em as string | Date | null | undefined) || null,
    };
  }

  private str(valor: unknown) {
    if (typeof valor !== 'string') {
      return null;
    }
    const texto = valor.trim();
    return texto || null;
  }

  private converterNumero(valor: unknown) {
    if (valor === null || valor === undefined) {
      return null;
    }
    const numero = Number(valor);
    return Number.isFinite(numero) ? numero : null;
  }

  private bool(valor: unknown): boolean | null {
    if (typeof valor === 'boolean') return valor;
    if (typeof valor === 'string') {
      const normalizado = valor.trim().toLowerCase();
      if (normalizado === 'true' || normalizado === 't' || normalizado === '1') return true;
      if (normalizado === 'false' || normalizado === 'f' || normalizado === '0') return false;
    }
    if (typeof valor === 'number') {
      if (valor === 1) return true;
      if (valor === 0) return false;
    }
    return null;
  }

  private tratarErroPersistencia(error: unknown, acao: string): never {
    if (error instanceof BadRequestException || error instanceof NotFoundException) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as {
        code?: string;
        detail?: string;
        message?: string;
      };

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

      if (erroPg.code === '23502') {
        throw new BadRequestException(
          'Campos obrigatorios nao foram informados para cadastrar/atualizar motorista.',
        );
      }

      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Permissao insuficiente no banco (RLS/sequence). Verifique policy da empresa e grants.',
        );
      }

      if (erroPg.code === '428C9') {
        throw new BadRequestException(
          'A API tentou inserir valor em coluna identity GENERATED ALWAYS. IDs auto incremento nao devem ser enviados no INSERT.',
        );
      }

      if (erroPg.code === '42703') {
        throw new BadRequestException(
          'Erro de estrutura no banco (coluna inexistente em SQL/trigger/function).',
        );
      }

      if (erroPg.code === '25P02') {
        throw new BadRequestException(
          'Transacao abortada no banco por erro SQL anterior. Verifique triggers/funcoes e o primeiro erro no log do banco.',
        );
      }

      const detalhe = [erroPg.code, erroPg.detail ?? erroPg.message]
        .filter((parte): parte is string => Boolean(parte))
        .join(' - ');

      if (detalhe) {
        throw new BadRequestException(
          `Falha ao ${acao} motorista no banco: ${detalhe}.`,
        );
      }
    }

    throw new BadRequestException(
      `Nao foi possivel ${acao} motorista neste momento.`,
    );
  }
}
