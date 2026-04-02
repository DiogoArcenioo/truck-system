import {
  BadRequestException,
  Injectable,
  Logger,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { createHmac, randomBytes, scryptSync, timingSafeEqual } from 'crypto';
import { QueryFailedError, Repository } from 'typeorm';
import { AtivarAssinaturaDto } from './dto/ativar-assinatura.dto';
import { AtualizarUsuarioSistemaDto } from './dto/atualizar-usuario-sistema.dto';
import { CriarUsuarioSistemaDto } from './dto/criar-usuario-sistema.dto';
import { CreateEmpresaDto } from './dto/create-empresa.dto';
import { CreateUsuarioDto } from './dto/create-usuario.dto';
import { LoginDto } from './dto/login.dto';
import { EmpresaEntity } from './entities/empresa.entity';
import { UsuarioEntity } from './entities/usuario.entity';
import { avaliarLicencaEmpresa } from './licenca.util';
import { PermissoesParciaisSistema, PermissoesSistema } from './permissoes.constants';
import { PermissoesService } from './permissoes.service';

@Injectable()
export class AuthService {
  private readonly logger = new Logger(AuthService.name);

  constructor(
    private readonly configService: ConfigService,
    @InjectRepository(EmpresaEntity)
    private readonly empresaRepository: Repository<EmpresaEntity>,
    @InjectRepository(UsuarioEntity)
    private readonly usuarioRepository: Repository<UsuarioEntity>,
    private readonly permissoesService: PermissoesService,
  ) {}

  async registrarEmpresa(dados: CreateEmpresaDto) {
    const payload = this.normalizarEntrada(dados);

    try {
      const empresa = await this.empresaRepository.manager.transaction(
        async (manager) => {
          const repositorio = manager.getRepository(EmpresaEntity);
          const codigo = await this.gerarProximoCodigo(repositorio);
          const slug = await this.gerarSlugUnico(
            repositorio,
            payload.nomeEmpresa,
          );

          const novaEmpresa = repositorio.create({
            codigo,
            nomeFantasia: payload.nomeEmpresa,
            razaoSocial: payload.razaoSocial,
            cnpj: payload.cnpj,
            emailPrincipal: payload.emailEmpresa,
            telefonePrincipal: payload.telefoneEmpresa,
            whatsappPrincipal: payload.telefoneEmpresa,
            ativo: payload.ativo,
            status: payload.status,
            plano: payload.plano,
            slug,
            usuarioAtualizacao: payload.usuarioAtualizacao,
          });

          return repositorio.save(novaEmpresa);
        },
      );

      return {
        sucesso: true,
        mensagem: 'Empresa cadastrada com sucesso.',
        empresa: {
          idEmpresa: Number(empresa.idEmpresa),
          codigo: empresa.codigo,
          nomeFantasia: empresa.nomeFantasia,
          slug: empresa.slug,
        },
        licenca: avaliarLicencaEmpresa({
          status: empresa.status,
          plano: empresa.plano,
          criadoEm: empresa.criadoEm,
        }),
      };
    } catch (error) {
      this.tratarErroCadastro(error);
    }
  }

  async registrarUsuario(dados: CreateUsuarioDto) {
    const payload = this.normalizarEntradaUsuario(dados);

    const empresaExiste = await this.empresaRepository.findOne({
      select: { idEmpresa: true },
      where: { idEmpresa: String(payload.idEmpresa) },
    });

    if (!empresaExiste) {
      throw new BadRequestException('Empresa informada nao existe.');
    }

    try {
      const usuario = await this.usuarioRepository.save(
        this.usuarioRepository.create({
          idEmpresa: String(payload.idEmpresa),
          nome: payload.nome,
          email: payload.email,
          senhaHash: this.gerarHashDaSenha(payload.senha),
          perfil: payload.perfil,
          ativo: payload.ativo,
          usuarioAtualizacao: payload.usuarioAtualizacao,
          ultimoLoginEm: null,
        }),
      );

      return {
        sucesso: true,
        mensagem: 'Usuario cadastrado com sucesso.',
        usuario: {
          idUsuario: Number(usuario.idUsuario),
          idEmpresa: Number(usuario.idEmpresa),
          nome: usuario.nome,
          email: usuario.email,
          perfil: usuario.perfil,
          ativo: usuario.ativo,
        },
      };
    } catch (error) {
      this.tratarErroCadastroUsuario(error);
    }
  }

  async login(dados: LoginDto) {
    const email = this.normalizarTextoMaiusculo(dados.email);
    const senha = dados.senha.trim();

    const usuario = await this.usuarioRepository.findOne({
      where: { email },
    });

    if (!usuario || !usuario.ativo) {
      throw new UnauthorizedException('Credenciais invalidas.');
    }

    const senhaValida = this.validarHashDaSenha(senha, usuario.senhaHash);
    if (!senhaValida) {
      throw new UnauthorizedException('Credenciais invalidas.');
    }

    const empresa = await this.empresaRepository.findOne({
      where: { idEmpresa: usuario.idEmpresa },
    });

    if (!empresa || !empresa.ativo) {
      throw new UnauthorizedException('Empresa vinculada nao disponivel.');
    }

    const licenca = avaliarLicencaEmpresa({
      status: empresa.status,
      plano: empresa.plano,
      criadoEm: empresa.criadoEm,
    });
    const permissoesEfetivas =
      await this.permissoesService.obterPermissoesEfetivasUsuario(
        Number(empresa.idEmpresa),
        Number(usuario.idUsuario),
        usuario.perfil,
      );
    const sessaoIniciadaEm = new Date();

    const expiresIn = this.parseJwtExpiresInToSeconds(
      this.configService.get<string>('JWT_EXPIRES_IN') ?? '15m',
    );

    const accessToken = this.assinarJwt(
      {
        sub: Number(usuario.idUsuario),
        idEmpresa: Number(empresa.idEmpresa),
        codigoEmpresa: empresa.codigo,
        nomeEmpresa: empresa.nomeFantasia,
        email: usuario.email,
        perfil: usuario.perfil,
        permissoes: permissoesEfetivas,
        sessao: sessaoIniciadaEm.getTime(),
        licencaModo: licenca.modoAcesso,
        licencaTerminoEm: licenca.trialTerminoEm ?? undefined,
        licencaDiasTrial: licenca.diasTrial,
        licencaDiasRestantes: licenca.diasRestantesTrial,
      },
      expiresIn,
    );

    await this.usuarioRepository.update(usuario.idUsuario, {
      ultimoLoginEm: sessaoIniciadaEm,
      usuarioAtualizacao: 'AUTH_LOGIN',
    });

    return {
      sucesso: true,
      mensagem: 'Login realizado com sucesso.',
      accessToken,
      tokenType: 'Bearer',
      expiresIn,
      usuario: {
        idUsuario: Number(usuario.idUsuario),
        nome: usuario.nome,
        email: usuario.email,
        perfil: usuario.perfil,
        permissoes: permissoesEfetivas,
        idEmpresa: Number(empresa.idEmpresa),
        codigoEmpresa: empresa.codigo,
      },
      licenca,
    };
  }

  async obterStatusLicenca(idEmpresa: number) {
    const empresa = await this.empresaRepository.findOne({
      where: { idEmpresa: String(idEmpresa) },
    });

    if (!empresa || !empresa.ativo) {
      throw new UnauthorizedException('Empresa vinculada nao disponivel.');
    }

    const licenca = avaliarLicencaEmpresa({
      status: empresa.status,
      plano: empresa.plano,
      criadoEm: empresa.criadoEm,
    });

    return {
      sucesso: true,
      licenca: {
        ...licenca,
        idEmpresa: Number(empresa.idEmpresa),
        codigoEmpresa: empresa.codigo,
      },
    };
  }

  async ativarAssinatura(dados: AtivarAssinaturaDto) {
    const empresa = await this.empresaRepository.findOne({
      where: { idEmpresa: String(dados.idEmpresa) },
    });

    if (!empresa) {
      throw new BadRequestException('Empresa informada nao existe.');
    }

    empresa.ativo = true;
    empresa.status = 'ativo';
    empresa.plano = this.normalizarTextoMinusculo(dados.plano ?? empresa.plano);
    empresa.usuarioAtualizacao = this.normalizarTextoMaiusculo(
      dados.usuarioAtualizacao ?? 'ASSINATURA_MANUAL',
    );

    const empresaAtualizada = await this.empresaRepository.save(empresa);
    const licenca = avaliarLicencaEmpresa({
      status: empresaAtualizada.status,
      plano: empresaAtualizada.plano,
      criadoEm: empresaAtualizada.criadoEm,
    });

    return {
      sucesso: true,
      mensagem: 'Assinatura ativada com sucesso.',
      empresa: {
        idEmpresa: Number(empresaAtualizada.idEmpresa),
        codigo: empresaAtualizada.codigo,
        status: empresaAtualizada.status,
        plano: empresaAtualizada.plano,
      },
      licenca,
    };
  }

  async listarUsuariosEmpresa(idEmpresa: number) {
    const usuarios = await this.usuarioRepository.find({
      where: { idEmpresa: String(idEmpresa) },
      order: { nome: 'ASC' },
    });
    const configuracaoPermissoes =
      await this.permissoesService.listarPermissoesEmpresa(idEmpresa);

    return {
      sucesso: true,
      total: usuarios.length,
      modulosPermissao: configuracaoPermissoes.modulos,
      permissoesPerfil: configuracaoPermissoes.perfis,
      usuarios: usuarios.map((usuario) => {
        const perfil = this.permissoesService.normalizarPerfil(usuario.perfil);
        const overrideUsuario =
          perfil === 'ADM'
            ? null
            : (configuracaoPermissoes.overridesUsuarios[
                Number(usuario.idUsuario)
              ] ?? null);
        const permissoesEfetivas =
          perfil === 'ADM'
            ? configuracaoPermissoes.perfis.ADM
            : this.permissoesService.combinarPermissoes(
                configuracaoPermissoes.perfis[perfil],
                overrideUsuario,
              );

        return this.mapearUsuarioSistema(usuario, {
          permissoesUsuario: overrideUsuario,
          permissoesEfetivas,
        });
      }),
    };
  }

  async atualizarPermissoesPerfilSistema(
    idEmpresa: number,
    perfil: string,
    permissoes: unknown,
    usuarioAtualizacao: string,
  ) {
    const perfilNormalizado = this.permissoesService.normalizarPerfil(perfil);
    const permissoesAtualizadas =
      await this.permissoesService.atualizarPermissoesPerfil(
        idEmpresa,
        perfilNormalizado,
        permissoes,
        this.normalizarTextoMaiusculo(usuarioAtualizacao),
      );

    return {
      sucesso: true,
      mensagem: `Permissoes do perfil ${perfilNormalizado} atualizadas com sucesso.`,
      perfil: perfilNormalizado,
      permissoes: permissoesAtualizadas,
    };
  }

  async atualizarPermissoesUsuarioSistema(
    idEmpresa: number,
    idUsuario: number,
    permissoes: unknown,
    usuarioAtualizacao: string,
  ) {
    const usuario = await this.usuarioRepository.findOne({
      where: { idUsuario: String(idUsuario), idEmpresa: String(idEmpresa) },
    });

    if (!usuario) {
      throw new BadRequestException(
        'Usuario nao encontrado para a empresa logada.',
      );
    }

    if (this.permissoesService.normalizarPerfil(usuario.perfil) === 'ADM') {
      throw new BadRequestException(
        'Usuarios com perfil ADM possuem acesso total fixo.',
      );
    }

    const permissoesUsuario =
      await this.permissoesService.atualizarPermissoesUsuario(
        idEmpresa,
        idUsuario,
        permissoes,
        this.normalizarTextoMaiusculo(usuarioAtualizacao),
      );

    const permissoesEfetivas =
      await this.permissoesService.obterPermissoesEfetivasUsuario(
        idEmpresa,
        idUsuario,
        usuario.perfil,
      );

    return {
      sucesso: true,
      mensagem: 'Permissoes customizadas do usuario atualizadas com sucesso.',
      usuario: this.mapearUsuarioSistema(usuario, {
        permissoesUsuario,
        permissoesEfetivas,
      }),
    };
  }

  async limparPermissoesUsuarioSistema(
    idEmpresa: number,
    idUsuario: number,
    usuarioAtualizacao: string,
  ) {
    const usuario = await this.usuarioRepository.findOne({
      where: { idUsuario: String(idUsuario), idEmpresa: String(idEmpresa) },
    });

    if (!usuario) {
      throw new BadRequestException(
        'Usuario nao encontrado para a empresa logada.',
      );
    }

    await this.permissoesService.limparPermissoesUsuario(idEmpresa, idUsuario);

    usuario.usuarioAtualizacao = this.normalizarTextoMaiusculo(
      usuarioAtualizacao,
    );
    await this.usuarioRepository.save(usuario);

    const permissoesEfetivas =
      await this.permissoesService.obterPermissoesEfetivasUsuario(
        idEmpresa,
        idUsuario,
        usuario.perfil,
      );

    return {
      sucesso: true,
      mensagem: 'Permissoes customizadas removidas com sucesso.',
      usuario: this.mapearUsuarioSistema(usuario, {
        permissoesUsuario: null,
        permissoesEfetivas,
      }),
    };
  }

  async cadastrarUsuarioSistema(
    idEmpresa: number,
    dados: CriarUsuarioSistemaDto,
    usuarioAtualizacao: string,
  ) {
    const perfilUsuarioCriado = this.normalizarPerfilSistema(dados.perfil);
    if (perfilUsuarioCriado === 'ADM' && dados.permissoesUsuario) {
      throw new BadRequestException(
        'Usuarios com perfil ADM possuem acesso total fixo e nao aceitam customizacao.',
      );
    }

    const resultado = await this.registrarUsuario({
      idEmpresa,
      nome: dados.nome,
      email: dados.email,
      senha: dados.senha,
      perfil: perfilUsuarioCriado,
      ativo: dados.ativo ?? true,
      usuarioAtualizacao,
    });

    const usuarioCriadoId = Number(resultado.usuario?.idUsuario ?? 0);

    if (
      dados.permissoesUsuario &&
      Number.isFinite(usuarioCriadoId) &&
      usuarioCriadoId > 0
    ) {
      await this.permissoesService.atualizarPermissoesUsuario(
        idEmpresa,
        usuarioCriadoId,
        dados.permissoesUsuario,
        usuarioAtualizacao,
      );
    }

    if (!Number.isFinite(usuarioCriadoId) || usuarioCriadoId <= 0) {
      return resultado;
    }

    const permissoesEfetivas =
      await this.permissoesService.obterPermissoesEfetivasUsuario(
        idEmpresa,
        usuarioCriadoId,
        perfilUsuarioCriado,
      );
    const permissoesUsuario =
      await this.permissoesService.obterPermissoesUsuarioCustomizadas(
        idEmpresa,
        usuarioCriadoId,
      );

    return {
      ...resultado,
      usuario: {
        ...resultado.usuario,
        permissoesUsuario,
        permissoesEfetivas,
      },
    };
  }

  async atualizarUsuarioSistema(
    idEmpresa: number,
    idUsuario: number,
    dados: AtualizarUsuarioSistemaDto,
    usuarioAtualizacao: string,
    idUsuarioLogado: number,
  ) {
    const usuario = await this.usuarioRepository.findOne({
      where: { idUsuario: String(idUsuario), idEmpresa: String(idEmpresa) },
    });

    if (!usuario) {
      throw new BadRequestException(
        'Usuario nao encontrado para a empresa logada.',
      );
    }

    const perfilDestino = this.normalizarPerfilSistema(
      dados.perfil ?? usuario.perfil,
    );
    if (perfilDestino === 'ADM' && dados.permissoesUsuario !== undefined) {
      throw new BadRequestException(
        'Usuarios com perfil ADM possuem acesso total fixo e nao aceitam customizacao.',
      );
    }

    if (dados.ativo === false && idUsuarioLogado === Number(usuario.idUsuario)) {
      throw new BadRequestException(
        'O usuario logado nao pode desativar o proprio acesso.',
      );
    }

    if (dados.nome !== undefined) {
      usuario.nome = this.normalizarTextoMaiusculo(dados.nome);
    }

    if (dados.email !== undefined) {
      usuario.email = this.normalizarTextoMaiusculo(dados.email);
    }

    if (dados.senha !== undefined) {
      const senha = dados.senha.trim();
      if (!this.senhaForte(senha)) {
        throw new BadRequestException(
          'A senha deve ter no minimo 8 caracteres, incluindo maiuscula, minuscula, numero e simbolo.',
        );
      }
      usuario.senhaHash = this.gerarHashDaSenha(senha);
    }

    if (dados.perfil !== undefined) {
      usuario.perfil = perfilDestino;
    }

    if (dados.ativo !== undefined) {
      usuario.ativo = dados.ativo;
    }

    usuario.usuarioAtualizacao = this.normalizarTextoMaiusculo(
      dados.usuarioAtualizacao ?? usuarioAtualizacao,
    );

    try {
      const atualizado = await this.usuarioRepository.save(usuario);

      if (dados.permissoesUsuario !== undefined) {
        await this.permissoesService.atualizarPermissoesUsuario(
          idEmpresa,
          Number(atualizado.idUsuario),
          dados.permissoesUsuario,
          usuario.usuarioAtualizacao,
        );
      }

      const permissoesUsuario =
        await this.permissoesService.obterPermissoesUsuarioCustomizadas(
          idEmpresa,
          Number(atualizado.idUsuario),
        );
      const permissoesEfetivas =
        await this.permissoesService.obterPermissoesEfetivasUsuario(
          idEmpresa,
          Number(atualizado.idUsuario),
          atualizado.perfil,
        );

      return {
        sucesso: true,
        mensagem: 'Usuario atualizado com sucesso.',
        usuario: this.mapearUsuarioSistema(atualizado, {
          permissoesUsuario,
          permissoesEfetivas,
        }),
      };
    } catch (error) {
      this.tratarErroCadastroUsuario(error);
    }
  }

  private normalizarEntrada(dados: CreateEmpresaDto): CreateEmpresaDto {
    return {
      ...dados,
      nomeEmpresa: this.normalizarTextoMaiusculo(dados.nomeEmpresa),
      razaoSocial: this.normalizarTextoMaiusculo(dados.razaoSocial),
      cnpj: this.formatarCnpj(dados.cnpj),
      emailEmpresa: this.normalizarTextoMaiusculo(dados.emailEmpresa),
      telefoneEmpresa: this.normalizarTextoMaiusculo(dados.telefoneEmpresa),
      ativo: dados.ativo ?? true,
      status: 'trial',
      plano: this.normalizarTextoMinusculo(dados.plano ?? 'basico'),
      usuarioAtualizacao: this.normalizarTextoMaiusculo(
        dados.usuarioAtualizacao ?? 'SISTEMA',
      ),
    };
  }

  private mapearUsuarioSistema(
    usuario: UsuarioEntity,
    extras?: {
      permissoesUsuario?: PermissoesParciaisSistema | null;
      permissoesEfetivas?: PermissoesSistema;
    },
  ) {
    return {
      idUsuario: Number(usuario.idUsuario),
      idEmpresa: Number(usuario.idEmpresa),
      nome: usuario.nome,
      email: usuario.email,
      perfil: usuario.perfil,
      ativo: usuario.ativo,
      ultimoLoginEm: usuario.ultimoLoginEm?.toISOString() ?? null,
      criadoEm: usuario.criadoEm?.toISOString() ?? null,
      atualizadoEm: usuario.atualizadoEm?.toISOString() ?? null,
      permissoesUsuario: extras?.permissoesUsuario ?? null,
      permissoesEfetivas: extras?.permissoesEfetivas,
    };
  }

  private normalizarPerfilSistema(perfil: string | undefined) {
    return this.permissoesService.normalizarPerfil(perfil);
  }

  private normalizarEntradaUsuario(dados: CreateUsuarioDto): CreateUsuarioDto {
    const senha = dados.senha.trim();

    if (!this.senhaForte(senha)) {
      throw new BadRequestException(
        'A senha deve ter no minimo 8 caracteres, incluindo maiuscula, minuscula, numero e simbolo.',
      );
    }

    return {
      ...dados,
      nome: this.normalizarTextoMaiusculo(dados.nome),
      email: this.normalizarTextoMaiusculo(dados.email),
      senha,
      perfil: this.normalizarPerfilSistema(dados.perfil ?? 'ADM'),
      ativo: dados.ativo ?? true,
      usuarioAtualizacao: (dados.usuarioAtualizacao ?? 'SISTEMA').trim(),
    };
  }

  private async gerarProximoCodigo(
    repositorio: Repository<EmpresaEntity>,
  ): Promise<string> {
    const resultado = await repositorio
      .createQueryBuilder('empresa')
      .select(
        `COALESCE(
          MAX(
            COALESCE(
              NULLIF(REGEXP_REPLACE(empresa.codigo, '[^0-9]', '', 'g'), ''),
              '0'
            )::BIGINT
          ),
          0
        ) + 1`,
        'proximo',
      )
      .getRawOne<{ proximo?: string | number }>();

    const proximoRaw = Number(resultado?.proximo ?? 1);
    const proximo =
      Number.isFinite(proximoRaw) && proximoRaw > 0 ? proximoRaw : 1;
    return `EMP-${String(proximo).padStart(4, '0')}`;
  }

  private async gerarSlugUnico(
    repositorio: Repository<EmpresaEntity>,
    nomeFantasia: string,
  ): Promise<string> {
    const base = this.slugify(nomeFantasia) || 'empresa';
    let candidato = base;

    for (let tentativa = 1; tentativa <= 500; tentativa += 1) {
      const existe = await repositorio.findOne({
        select: { idEmpresa: true },
        where: { slug: candidato },
      });

      if (!existe) {
        return candidato;
      }

      candidato = `${base}-${tentativa + 1}`;
    }

    throw new BadRequestException(
      'Nao foi possivel gerar slug unico para a empresa.',
    );
  }

  private tratarErroCadastro(error: unknown): never {
    if (error instanceof BadRequestException) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as {
        code?: string;
        message?: string;
      };

      this.logger.error(
        `Falha ao registrar empresa. code=${erroPg.code ?? 'N/A'} message=${erroPg.message ?? 'Erro desconhecido'}`,
      );

      if (erroPg.code === '23505') {
        throw new BadRequestException(
          'Ja existe empresa cadastrada com os dados informados (cnpj, email, codigo ou slug).',
        );
      }

      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Usuario do banco sem permissao para inserir empresa na tabela app.empresas.',
        );
      }

      if (erroPg.code === '42P01') {
        throw new BadRequestException('Tabela app.empresas nao encontrada.');
      }

      if (erroPg.code === '42703') {
        throw new BadRequestException(
          'Estrutura da tabela app.empresas esta diferente do esperado.',
        );
      }

      if (erroPg.code === '23514') {
        throw new BadRequestException(
          'Dados invalidos para status/plano da empresa. Valores permitidos de status: ativo, inativo, bloqueado, cancelado, trial. Valores permitidos de plano: basico, pro, premium, enterprise.',
        );
      }
    }

    this.logger.error(
      `Falha ao registrar empresa sem codigo SQL mapeado. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
    );
    throw new BadRequestException(
      'Nao foi possivel cadastrar a empresa neste momento.',
    );
  }

  private tratarErroCadastroUsuario(error: unknown): never {
    if (error instanceof BadRequestException) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as {
        code?: string;
        message?: string;
      };

      this.logger.error(
        `Falha ao registrar usuario. code=${erroPg.code ?? 'N/A'} message=${erroPg.message ?? 'Erro desconhecido'}`,
      );

      if (erroPg.code === '23505') {
        throw new BadRequestException(
          'Ja existe usuario cadastrado com esse e-mail.',
        );
      }

      if (erroPg.code === '23503') {
        throw new BadRequestException('Empresa informada nao existe.');
      }

      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Usuario do banco sem permissao para inserir em app.usuarios.',
        );
      }

      if (erroPg.code === '42P01') {
        throw new BadRequestException('Tabela app.usuarios nao encontrada.');
      }

      if (erroPg.code === '42703') {
        throw new BadRequestException(
          'Estrutura da tabela app.usuarios esta diferente do esperado.',
        );
      }

      if (erroPg.code === '23514') {
        throw new BadRequestException(
          'Dados invalidos para o usuario. Revise perfil e campos obrigatorios.',
        );
      }
    }

    this.logger.error(
      `Falha ao registrar usuario sem codigo SQL mapeado. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
    );
    throw new BadRequestException(
      'Nao foi possivel cadastrar o usuario neste momento.',
    );
  }

  private validarHashDaSenha(senha: string, senhaHash: string): boolean {
    try {
      const [algoritmo, paramsRaw, saltRaw, hashRaw] = senhaHash.split('$');

      if (algoritmo !== 'scrypt' || !paramsRaw || !saltRaw || !hashRaw) {
        return false;
      }

      const params = this.parseScryptParams(paramsRaw);
      if (!params) {
        return false;
      }

      const salt = Buffer.from(saltRaw, 'base64');
      const hashEsperado = Buffer.from(hashRaw, 'base64');

      if (salt.length === 0 || hashEsperado.length === 0) {
        return false;
      }

      const hashCalculado = scryptSync(senha, salt, hashEsperado.length, {
        N: params.N,
        r: params.r,
        p: params.p,
      });

      if (hashCalculado.length !== hashEsperado.length) {
        return false;
      }

      return timingSafeEqual(hashCalculado, hashEsperado);
    } catch {
      return false;
    }
  }

  private parseScryptParams(
    paramsRaw: string,
  ): { N: number; r: number; p: number } | null {
    const entries = paramsRaw.split(',').map((part) => part.trim());
    const map = new Map<string, number>();

    for (const entry of entries) {
      const [key, value] = entry.split('=');
      const parsed = Number(value);
      if (!key || !Number.isFinite(parsed) || parsed <= 0) {
        return null;
      }
      map.set(key, parsed);
    }

    const N = map.get('N');
    const r = map.get('r');
    const p = map.get('p');

    if (!N || !r || !p) {
      return null;
    }

    return { N, r, p };
  }

  private assinarJwt(
    payload: {
      sub: number;
      idEmpresa: number;
      codigoEmpresa: string;
      nomeEmpresa?: string;
      email: string;
      perfil: string;
      permissoes?: PermissoesSistema;
      sessao?: number;
      licencaModo?: string;
      licencaTerminoEm?: string;
      licencaDiasTrial?: number;
      licencaDiasRestantes?: number;
    },
    expiresInSeconds: number,
  ): string {
    const jwtSecret = this.configService.get<string>('JWT_SECRET')?.trim();

    if (!jwtSecret) {
      throw new UnauthorizedException(
        'JWT_SECRET nao configurado no servidor.',
      );
    }

    const now = Math.floor(Date.now() / 1000);
    const jti = randomBytes(16).toString('hex');

    const header = {
      alg: 'HS256',
      typ: 'JWT',
    };

    const claims = {
      ...payload,
      iss: 'truck-system',
      iat: now,
      exp: now + expiresInSeconds,
      jti,
    };

    const encodedHeader = this.toBase64Url(JSON.stringify(header));
    const encodedClaims = this.toBase64Url(JSON.stringify(claims));
    const signingInput = `${encodedHeader}.${encodedClaims}`;

    const signature = createHmac('sha256', jwtSecret)
      .update(signingInput)
      .digest('base64url');

    return `${signingInput}.${signature}`;
  }

  private parseJwtExpiresInToSeconds(raw: string): number {
    const normalized = raw.trim().toLowerCase();
    const match = normalized.match(/^(\d+)([smhd]?)$/);

    if (!match) {
      return 900;
    }

    const value = Number(match[1]);
    const unit = match[2] || 's';

    if (!Number.isFinite(value) || value <= 0) {
      return 900;
    }

    if (unit === 'm') {
      return value * 60;
    }

    if (unit === 'h') {
      return value * 3600;
    }

    if (unit === 'd') {
      return value * 86400;
    }

    return value;
  }

  private toBase64Url(value: string): string {
    return Buffer.from(value, 'utf8').toString('base64url');
  }

  private gerarHashDaSenha(senha: string): string {
    const salt = randomBytes(16);
    const hash = scryptSync(senha, salt, 64);
    return `scrypt$N=16384,r=8,p=1$${salt.toString('base64')}$${hash.toString('base64')}`;
  }

  private senhaForte(senha: string): boolean {
    return (
      senha.length >= 8 &&
      /[A-Z]/.test(senha) &&
      /[a-z]/.test(senha) &&
      /\d/.test(senha) &&
      /[^A-Za-z0-9\s]/.test(senha)
    );
  }

  private slugify(texto: string): string {
    return texto
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .replace(/-+/g, '-');
  }

  private normalizarTextoMaiusculo(valor: string): string {
    return valor.trim().toUpperCase();
  }

  private normalizarTextoMinusculo(valor: string): string {
    return valor.trim().toLowerCase();
  }

  private formatarCnpj(cnpj: string): string {
    const apenasNumeros = cnpj.replace(/\D/g, '');

    if (apenasNumeros.length !== 14) {
      throw new BadRequestException('CNPJ deve conter 14 digitos.');
    }

    return apenasNumeros.replace(
      /^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})$/,
      '$1.$2.$3/$4-$5',
    );
  }
}
