type RuntimeConfig={authMode?:'entra'|'demo';apiUrl?:string;clientId?:string;tenantId?:string;backendScope?:string};
const runtime=(window as Window & {__BD_CONFIG__?:RuntimeConfig}).__BD_CONFIG__??{};
const tenant=runtime.tenantId??'f402d005-2163-4d89-a6cd-e07c25b973b2';
export const environment={
  production:true,
  authMode:runtime.authMode??'entra',
  apiUrl:runtime.apiUrl||window.location.origin,
  azure:{clientId:runtime.clientId??'2412c4b5-8d7b-46ab-8887-bc06a42a72fa',tenantId:tenant,
    authority:`https://login.microsoftonline.com/${tenant}`,redirectUri:window.location.origin+'/',
    backendScope:runtime.backendScope??'api://acc38f45-b5ba-4e55-9b14-fcaa00f9c661/OT.Create'}
};
