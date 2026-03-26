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
import { CreateEmpresaDto } from './dto/create-empresa.dto';
import { CreateUsuarioDto } from './dto/create-usuario.dto';
import { LoginDto } from './dto/login.dto';
import { EmpresaEntity } from './entities/empresa.entity';
import { UsuarioEntity } from './entities/usuario.entity';

@Injectable()
export class AuthService {
  private readonly logger = new Logger(AuthService.name);

  constructor(
    private readonly configService: ConfigService,
    @InjectRepository(EmpresaEntity)
    private readonly empresaRepository: Repository<EmpresaEntity>,
    @InjectRepository(UsuarioEntity)
    private readonly usuarioRepository: Repository<UsuarioEntity>,
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

    const expiresIn = this.parseJwtExpiresInToSeconds(
      this.configService.get<string>('JWT_EXPIRES_IN') ?? '15m',
    );

    const accessToken = this.assinarJwt({
      sub: Number(usuario.idUsuario),
      idEmpresa: Number(empresa.idEmpresa),
      codigoEmpresa: empresa.codigo,
      email: usuario.email,
      perfil: usuario.perfil,
    }, expiresIn);

    await this.usuarioRepository.update(usuario.idUsuario, {
      ultimoLoginEm: new Date(),
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
        idEmpresa: Number(empresa.idEmpresa),
        codigoEmpresa: empresa.codigo,
      },
    };
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
      status: this.normalizarTextoMinusculo(dados.status ?? 'ativo'),
      plano: this.normalizarTextoMinusculo(dados.plano ?? 'basico'),
      usuarioAtualizacao: this.normalizarTextoMaiusculo(
        dados.usuarioAtualizacao ?? 'SISTEMA',
      ),
    };
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
      perfil: (dados.perfil ?? 'ADM').trim().toUpperCase(),
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
      email: string;
      perfil: string;
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
