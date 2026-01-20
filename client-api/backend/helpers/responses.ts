import {StatusCodes} from 'http-status-codes'
import { Response } from 'express'

export const createResponse = (res: Response, message: string, data: any): Response => res
  .status(StatusCodes.OK)
  .json({message, data})

export const createErrorResponse = (res: Response, status: number, error: string): Response => res
  .status(status)
  .json({error})
