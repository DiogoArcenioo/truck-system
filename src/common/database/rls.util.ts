import { EntityManager } from 'typeorm';

export async function configurarContextoEmpresaRls(
  manager: EntityManager,
  idEmpresa: number,
): Promise<void> {
  await manager.query(
    "SELECT set_config('app.empresa_id', $1, true), set_config('app.current_empresa_id', $1, true)",
    [String(idEmpresa)],
  );
}
