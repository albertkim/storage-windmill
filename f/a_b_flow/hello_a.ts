export async function main(): Promise<{ step: string; message: string }> {
  const message = "Hello world from A";
  console.log(message);
  return { step: "a", message };
}
