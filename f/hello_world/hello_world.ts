export async function main(): Promise<{ message: string }> {
  const message = "Hello, world!";
  console.log(message);
  return { message };
}
