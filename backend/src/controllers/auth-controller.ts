import bcrypt from "bcryptjs";
import type { Request, Response } from "express";
import { prisma } from "../db.js";
import { authSchema } from "../types/auth-schema.js";
import { createToken } from "../utils/auth.js";
import { sendValidationError } from "../utils/validation.js";
import jwt from "jsonwebtoken"

export async function signup(req: Request, res: Response): Promise<void> {
  const parsedBody = authSchema.safeParse(req.body);
  if (!parsedBody.success) {
    sendValidationError(res, parsedBody.error);
    return;
  }

  const { username, password } = parsedBody.data;
  const hashedPassword = await bcrypt.hash(password, 10);

  try {
    const user = await prisma.user.create({
      data: {
        username,
        password: hashedPassword,
      },
    });

    res.status(201).json({
      token: createToken({ userId: user.id }),
      userId: user.id,
      username: user.username,
    });
  } catch (e) {
    console.log(e);
    res.status(409).json({ error: "username already exists" });
  }
}

export async function signin(req: Request, res: Response): Promise<void> {
  //TODO: Implement signin logic
  const parsedBody = authSchema.safeParse(req.body);
  if (!parsedBody.success) {
    sendValidationError(res, parsedBody.error);
    return;
  }

  const { username, password } = parsedBody.data;
  try {
    const user = await prisma.user.findUnique({
      where: { username }
    });

    if (!user) {
      res.status(404).json({ error: "User not found" });
      return;
    }
    const isPasswordValid = bcrypt.compare(password, user.password);

    if (!isPasswordValid) {
      res.status(404).json({ error: "User not found" });
    }

    const token = jwt.sign({ userId: user.id }, process.env.JWT_SECRET as string, { expiresIn: "7d" });

    res.status(200).json({ token, message: "Logged in" })

  } catch (error) {
    res.status(500).json({ error: "internal server error" });
  }

}
