import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { QueryFailedError, Repository } from 'typeorm';
import { CreateEmpresaDto } from './dto/create-empresa.dto';
import { EmpresaEntity } from './entities/empresa.entity';

@Injectable()
export class AuthService {
  private readonly logger = new Logger(AuthService.name);

  constructor(
    @InjectRepository(EmpresaEntity)
    private readonly empresaRepository: Repository<EmpresaEntity>,
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
            payload.nomeFantasia,
          );

          const novaEmpresa = repositorio.create({
            codigo,
            nomeFantasia: payload.nomeFantasia,
            razaoSocial: payload.razaoSocial,
            cnpj: payload.cnpj,
            emailPrincipal: payload.emailPrincipal,
            telefonePrincipal: payload.telefonePrincipal,
            whatsappPrincipal: payload.whatsappPrincipal,
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

  private normalizarEntrada(dados: CreateEmpresaDto): CreateEmpresaDto {
    return {
      ...dados,
      nomeFantasia: dados.nomeFantasia.trim(),
      razaoSocial: dados.razaoSocial.trim(),
      cnpj: dados.cnpj.replace(/\D/g, ''),
      emailPrincipal: dados.emailPrincipal.trim().toLowerCase(),
      telefonePrincipal: dados.telefonePrincipal.trim(),
      whatsappPrincipal: (
        dados.whatsappPrincipal ?? dados.telefonePrincipal
      ).trim(),
      ativo: dados.ativo ?? true,
      status: (dados.status ?? 'ativo').trim().toLowerCase(),
      plano: (dados.plano ?? 'basico').trim().toLowerCase(),
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
    }

    this.logger.error(
      `Falha ao registrar empresa sem codigo SQL mapeado. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
    );
    throw new BadRequestException(
      'Nao foi possivel cadastrar a empresa neste momento.',
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
}
